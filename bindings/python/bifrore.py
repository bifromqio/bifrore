import ctypes
import json
from ctypes import c_char_p, c_void_p, c_int, c_uint16, c_uint32, c_size_t, c_bool


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
    ):
        self.lib = ctypes.cdll.LoadLibrary(lib_path)
        self._setup_signatures()
        self.handle = self.lib.bre_create_with_rules(rule_path.encode("utf-8"))
        if not self.handle:
            raise RuntimeError("Failed to create engine with rule file")
        self._callback = None
        self._callback_c = None
        self._log_callback = None
        self._log_callback_c = None
        self._mqtt_started = False
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
            c_void_p,
            c_void_p,
        ]
        self.lib.bre_start_mqtt.restype = c_int
        self.lib.bre_stop_mqtt.argtypes = [c_void_p]
        self.lib.bre_stop_mqtt.restype = c_int
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

    def on_message(self, handler, executor=None, loop=None):
        CALLBACK = ctypes.CFUNCTYPE(
            None,
            c_void_p,
            c_char_p,
            ctypes.POINTER(ctypes.c_ubyte),
            c_size_t,
            c_char_p,
        )

        def _callback(user_data, rule_id, payload, payload_len, destinations_json):
            _ = user_data
            payload_bytes = bytes(payload[: payload_len])
            destinations = json.loads(destinations_json.decode("utf-8"))
            args = (rule_id.decode("utf-8"), payload_bytes, destinations)
            if loop is not None:
                loop.call_soon_threadsafe(handler, *args)
            elif executor is not None:
                executor.submit(handler, *args)
            else:
                handler(*args)

        self._callback = handler
        self._callback_c = CALLBACK(_callback)
        if not self._mqtt_started:
            rc = self._start_mqtt()
            if rc != 0:
                raise RuntimeError(f"Failed to start MQTT, error code: {rc}")
            self._mqtt_started = True

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
        if self._callback_c is None:
            raise RuntimeError("on_message handler not set")
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
            self._callback_c,
            None,
        )

    def _stop_mqtt(self):
        return self.lib.bre_stop_mqtt(self.handle)

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
        if self.handle:
            self._stop_mqtt()
            self._mqtt_started = False
        if self.handle:
            self.lib.bre_destroy(self.handle)
            self.handle = None


if __name__ == "__main__":
    # Example usage (adjust library path)
    engine = BifroRE("./libbifrore_embed.dylib", "./rule.json")
    engine.on_message(lambda rule_id, payload, destinations: print(rule_id, destinations))

    engine.close()
