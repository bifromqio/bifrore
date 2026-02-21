import asyncio
import ctypes
import json
import threading
from dataclasses import dataclass
from ctypes import c_bool, c_char_p, c_int, c_size_t, c_uint16, c_uint32, c_void_p


class _EvalResult(ctypes.Structure):
    _fields_ = [
        ("rule_id", c_char_p),
        ("payload", ctypes.POINTER(ctypes.c_ubyte)),
        ("payload_len", c_size_t),
        ("destinations_json", c_char_p),
    ]


@dataclass
class EvalMessage:
    rule_id: str
    payload: bytes
    destinations: list


class BifroRE:
    def __init__(
        self,
        lib_path,
        rule_path,
        host="127.0.0.1",
        port=1883,
        client_prefix="bifrore-embed",
        node_id=None,
        client_count=1,
        group_name="bifrore-group",
        username=None,
        password=None,
        clean_start=True,
        session_expiry_interval=3600,
        ordered=False,
        ordered_prefix="",
        keep_alive_secs=30,
        multi_nci=False,
    ):
        self.lib = ctypes.cdll.LoadLibrary(lib_path)
        self._setup_signatures()
        self.handle = self.lib.bre_create_with_rules(rule_path.encode("utf-8"))
        if not self.handle:
            raise RuntimeError("Failed to create engine with rule file")
        self._log_callback = None
        self._log_callback_c = None
        self._mqtt_started = False
        self._poll_thread = None
        self._poll_stop = threading.Event()
        self._loop = None
        self._queue = None
        self._mqtt_config = {
            "host": host,
            "port": port,
            "client_prefix": client_prefix,
            "node_id": node_id,
            "client_count": client_count,
            "group_name": group_name,
            "username": username,
            "password": password,
            "clean_start": clean_start,
            "session_expiry_interval": session_expiry_interval,
            "ordered": ordered,
            "ordered_prefix": ordered_prefix,
            "keep_alive_secs": keep_alive_secs,
            "multi_nci": multi_nci,
        }

    def _setup_signatures(self):
        self.lib.bre_create_with_rules.argtypes = [c_char_p]
        self.lib.bre_create_with_rules.restype = c_void_p
        self.lib.bre_destroy.argtypes = [c_void_p]
        self.lib.bre_start_mqtt.argtypes = [
            c_void_p,
            c_char_p,
            c_uint16,
            c_char_p,
            c_char_p,
            c_uint16,
            c_char_p,
            c_char_p,
            c_bool,
            c_uint32,
            c_char_p,
            c_bool,
            c_char_p,
            c_uint16,
            c_bool,
            c_void_p,
            c_void_p,
        ]
        self.lib.bre_start_mqtt.restype = c_int
        self.lib.bre_stop_mqtt.argtypes = [c_void_p]
        self.lib.bre_stop_mqtt.restype = c_int
        self.lib.bre_poll_eval_result.argtypes = [
            c_void_p,
            c_uint32,
            ctypes.POINTER(_EvalResult),
        ]
        self.lib.bre_poll_eval_result.restype = c_int
        self.lib.bre_free_eval_result.argtypes = [ctypes.POINTER(_EvalResult)]
        self.lib.bre_free_eval_result.restype = None
        self.lib.bre_metrics_snapshot.argtypes = [
            c_void_p,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
        ]
        self.lib.bre_metrics_snapshot.restype = c_int
        self.lib.bre_set_log_callback.argtypes = [c_void_p, c_void_p, c_int]
        self.lib.bre_set_log_callback.restype = c_int

    async def __aenter__(self):
        await self._ensure_started()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
        self.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._queue is None:
            raise StopAsyncIteration
        item = await self._queue.get()
        if item is None:
            raise StopAsyncIteration
        return item

    async def _ensure_started(self):
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        if self._queue is None:
            self._queue = asyncio.Queue()
        if not self._mqtt_started:
            rc = self._start_mqtt()
            if rc != 0:
                raise RuntimeError(f"Failed to start MQTT, error code: {rc}")
            self._mqtt_started = True
        self._start_poll_loop()

    def on_log(self, handler, min_level=3, executor=None, loop=None):
        LOG_CALLBACK = ctypes.CFUNCTYPE(
            None,
            c_void_p,
            c_int,
            c_char_p,
            c_char_p,
            ctypes.c_uint64,
            c_char_p,
            c_char_p,
            c_char_p,
            c_int,
        )

        if handler is None:
            self._log_callback = None
            self._log_callback_c = None
            return self.lib.bre_set_log_callback(None, None, min_level)

        def _log_callback(
            user_data,
            level,
            target,
            message,
            timestamp_millis,
            thread_id,
            module_path,
            file,
            line,
        ):
            _ = user_data
            args = (
                level,
                target.decode("utf-8") if target else "bifrore",
                message.decode("utf-8") if message else "",
                timestamp_millis,
                thread_id.decode("utf-8") if thread_id else "unknown-thread",
                module_path.decode("utf-8") if module_path else "",
                file.decode("utf-8") if file else "",
                line,
            )
            if loop is not None:
                loop.call_soon_threadsafe(handler, *args)
            elif executor is not None:
                executor.submit(handler, *args)
            else:
                handler(*args)

        self._log_callback = handler
        self._log_callback_c = LOG_CALLBACK(_log_callback)
        return self.lib.bre_set_log_callback(self._log_callback_c, None, min_level)

    def _start_mqtt(self):
        cfg = self._mqtt_config
        return self.lib.bre_start_mqtt(
            self.handle,
            cfg["host"].encode("utf-8"),
            cfg["port"],
            cfg["client_prefix"].encode("utf-8"),
            cfg["node_id"].encode("utf-8") if cfg["node_id"] else None,
            cfg["client_count"],
            cfg["username"].encode("utf-8") if cfg["username"] else None,
            cfg["password"].encode("utf-8") if cfg["password"] else None,
            cfg["clean_start"],
            cfg["session_expiry_interval"],
            cfg["group_name"].encode("utf-8"),
            cfg["ordered"],
            cfg["ordered_prefix"].encode("utf-8"),
            cfg["keep_alive_secs"],
            cfg["multi_nci"],
            None,
            None,
        )

    def _stop_mqtt(self):
        return self.lib.bre_stop_mqtt(self.handle)

    def _start_poll_loop(self):
        if self._poll_thread is not None and self._poll_thread.is_alive():
            return
        self._poll_stop.clear()

        def _poll():
            while not self._poll_stop.is_set() and self.handle:
                result = _EvalResult()
                rc = self.lib.bre_poll_eval_result(self.handle, 0xFFFFFFFF, ctypes.byref(result))
                if rc != 1:
                    if rc == -3:
                        break
                    continue
                try:
                    rule_id = result.rule_id.decode("utf-8") if result.rule_id else ""
                    payload = (
                        ctypes.string_at(result.payload, result.payload_len)
                        if result.payload and result.payload_len > 0
                        else b""
                    )
                    destinations_raw = (
                        result.destinations_json.decode("utf-8")
                        if result.destinations_json
                        else "[]"
                    )
                    destinations = json.loads(destinations_raw)
                    message = EvalMessage(rule_id=rule_id, payload=payload, destinations=destinations)
                    if self._loop is not None and self._queue is not None:
                        self._loop.call_soon_threadsafe(self._queue.put_nowait, message)
                finally:
                    self.lib.bre_free_eval_result(ctypes.byref(result))

        self._poll_thread = threading.Thread(target=_poll, name="bifrore-poller", daemon=True)
        self._poll_thread.start()

    async def stop(self):
        if self.handle and self._mqtt_started:
            self._stop_mqtt()
            self._mqtt_started = False
        self._poll_stop.set()
        if self._poll_thread is not None:
            await asyncio.to_thread(self._poll_thread.join, 1.0)
            self._poll_thread = None
        if self._queue is not None:
            await self._queue.put(None)

    def metrics(self):
        eval_count = ctypes.c_uint64()
        eval_error_count = ctypes.c_uint64()
        eval_total_nanos = ctypes.c_uint64()
        eval_max_nanos = ctypes.c_uint64()
        rc = self.lib.bre_metrics_snapshot(
            self.handle,
            ctypes.byref(eval_count),
            ctypes.byref(eval_error_count),
            ctypes.byref(eval_total_nanos),
            ctypes.byref(eval_max_nanos),
        )
        if rc != 0:
            return None
        return {
            "eval_count": eval_count.value,
            "eval_error_count": eval_error_count.value,
            "eval_total_nanos": eval_total_nanos.value,
            "eval_max_nanos": eval_max_nanos.value,
        }

    def close(self):
        self.lib.bre_set_log_callback(None, None, 3)
        if self.handle and self._mqtt_started:
            self._stop_mqtt()
            self._mqtt_started = False
        self._poll_stop.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=1.0)
            self._poll_thread = None
        if self.handle:
            self.lib.bre_destroy(self.handle)
            self.handle = None


if __name__ == "__main__":
    async def main():
        async with BifroRE("./libbifrore_embed.dylib", "./rule.json") as msg_stream:
            async for msg in msg_stream:
                print(f"msg: {msg}")

    asyncio.run(main())
