import asyncio
import ctypes
import json
import os
from ctypes import c_bool, c_char_p, c_int, c_size_t, c_uint16, c_uint32, c_void_p

BRE_OK = 0
BRE_POLL_RESULT_READY = 1

BRE_ERR_INVALID_ARGUMENT = -1
BRE_ERR_INVALID_STATE = -2
BRE_ERR_OPERATION_FAILED = -3
BRE_ERR_START_FAILED = -4
BRE_ERR_ALREADY_STARTED = -5
BRE_ERR_WORKER_UNAVAILABLE = -6


class _PackedEvalResults(ctypes.Structure):
    _fields_ = [
        ("rule_indices", ctypes.POINTER(c_uint32)),
        ("payload_offsets", ctypes.POINTER(c_uint32)),
        ("payload_lengths", ctypes.POINTER(c_uint32)),
        ("payload_data", ctypes.POINTER(ctypes.c_ubyte)),
        ("len", c_size_t),
        ("payload_data_len", c_size_t),
    ]


class _RuleMetadata(ctypes.Structure):
    _fields_ = [
        ("rule_index", c_uint16),
        ("destinations", ctypes.POINTER(c_char_p)),
        ("destinations_len", c_size_t),
    ]


class _MetricsSnapshot(ctypes.Structure):
    _fields_ = [
        ("ingress_message_count", ctypes.c_uint64),
        ("core_queue_depth", ctypes.c_uint64),
        ("core_queue_depth_max", ctypes.c_uint64),
        ("core_queue_drop_count", ctypes.c_uint64),
        ("ffi_queue_depth", ctypes.c_uint64),
        ("ffi_queue_depth_max", ctypes.c_uint64),
        ("ffi_queue_drop_count", ctypes.c_uint64),
        ("message_pipeline_count", ctypes.c_uint64),
        ("message_pipeline_total_nanos", ctypes.c_uint64),
        ("message_pipeline_max_nanos", ctypes.c_uint64),
        ("eval_count", ctypes.c_uint64),
        ("eval_error_count", ctypes.c_uint64),
        ("exec_count", ctypes.c_uint64),
        ("exec_total_nanos", ctypes.c_uint64),
        ("exec_max_nanos", ctypes.c_uint64),
        ("topic_match_count", ctypes.c_uint64),
        ("topic_match_total_nanos", ctypes.c_uint64),
        ("topic_match_max_nanos", ctypes.c_uint64),
        ("payload_decode_count", ctypes.c_uint64),
        ("payload_decode_total_nanos", ctypes.c_uint64),
        ("payload_decode_max_nanos", ctypes.c_uint64),
        ("msg_ir_build_count", ctypes.c_uint64),
        ("msg_ir_build_total_nanos", ctypes.c_uint64),
        ("msg_ir_build_max_nanos", ctypes.c_uint64),
        ("fast_where_count", ctypes.c_uint64),
        ("fast_where_total_nanos", ctypes.c_uint64),
        ("fast_where_max_nanos", ctypes.c_uint64),
        ("predicate_count", ctypes.c_uint64),
        ("predicate_total_nanos", ctypes.c_uint64),
        ("predicate_max_nanos", ctypes.c_uint64),
        ("projection_count", ctypes.c_uint64),
        ("projection_total_nanos", ctypes.c_uint64),
        ("projection_max_nanos", ctypes.c_uint64),
    ]

class BifroRE:
    PAYLOAD_JSON = 1
    PAYLOAD_PROTOBUF = 2
    NOTIFY_MODE_POLL = 0
    NOTIFY_MODE_PUSH = 1

    def __init__(
        self,
        lib_path_or_rule_path,
        rule_path=None,
        host="127.0.0.1",
        port=1883,
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
        payload_format=PAYLOAD_JSON,
        client_ids_path="./client_ids",
        protobuf_descriptor_set_path=None,
        protobuf_message_name=None,
    ):
        lib_path, rule_path = self._resolve_paths(lib_path_or_rule_path, rule_path)
        self.lib = ctypes.cdll.LoadLibrary(lib_path)
        self._setup_signatures()
        self.handle = self.lib.bre_create_engine(
            rule_path.encode("utf-8"),
            payload_format,
            client_ids_path.encode("utf-8") if client_ids_path else None,
            self.NOTIFY_MODE_PUSH,
            protobuf_descriptor_set_path.encode("utf-8") if protobuf_descriptor_set_path else None,
            protobuf_message_name.encode("utf-8") if protobuf_message_name else None,
        )
        if not self.handle:
            raise RuntimeError("Failed to create engine with rule file")
        self._log_callback = None
        self._log_callback_c = None
        self._mqtt_started = False
        self._loop = None
        self._queue = None
        self._notify_fd = -1
        self._reader_attached = False
        self._mqtt_config = {
            "host": host,
            "port": port,
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
        self._rule_metadata = self._load_rule_metadata()

    @staticmethod
    def _resolve_paths(lib_path_or_rule_path, rule_path):
        if rule_path is None:
            rule_path = lib_path_or_rule_path
            lib_path = BifroRE._default_lib_path()
        else:
            lib_path = lib_path_or_rule_path
        return lib_path, rule_path

    @staticmethod
    def _default_lib_path():
        module_dir = os.path.dirname(os.path.abspath(__file__))
        ext = ".dylib" if os.uname().sysname == "Darwin" else ".so"
        candidate = os.path.join(module_dir, f"libbifrore_embed{ext}")
        if not os.path.exists(candidate):
            raise RuntimeError(f"Bundled native library not found: {candidate}")
        return candidate

    def _setup_signatures(self):
        self.lib.bre_create_engine.argtypes = [
            c_char_p,
            c_int,
            c_char_p,
            c_int,
            c_char_p,
            c_char_p,
        ]
        self.lib.bre_create_engine.restype = c_void_p
        self.lib.bre_destroy.argtypes = [c_void_p]
        self.lib.bre_start_mqtt.argtypes = [
            c_void_p,
            c_char_p,
            c_uint16,
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
        ]
        self.lib.bre_start_mqtt.restype = c_int
        self.lib.bre_get_notify_fd.argtypes = [c_void_p]
        self.lib.bre_get_notify_fd.restype = c_int
        self.lib.bre_poll_eval_results_packed.argtypes = [
            c_void_p,
            c_uint32,
            ctypes.POINTER(_PackedEvalResults),
        ]
        self.lib.bre_poll_eval_results_packed.restype = c_int
        self.lib.bre_free_packed_eval_results.argtypes = [ctypes.POINTER(_PackedEvalResults)]
        self.lib.bre_free_packed_eval_results.restype = None
        self.lib.bre_get_rule_metadata_table.argtypes = [
            c_void_p,
            ctypes.POINTER(ctypes.POINTER(_RuleMetadata)),
            ctypes.POINTER(c_size_t),
        ]
        self.lib.bre_get_rule_metadata_table.restype = c_int
        self.lib.bre_free_rule_metadata_table.argtypes = [ctypes.POINTER(_RuleMetadata), c_size_t]
        self.lib.bre_free_rule_metadata_table.restype = None
        self.lib.bre_metrics_snapshot.argtypes = [
            c_void_p,
            ctypes.POINTER(_MetricsSnapshot),
        ]
        self.lib.bre_metrics_snapshot.restype = c_int
        self.lib.bre_set_log_callback.argtypes = [c_void_p, c_void_p, c_int]
        self.lib.bre_set_log_callback.restype = c_int

    def _load_rule_metadata(self):
        out_metadata = ctypes.POINTER(_RuleMetadata)()
        out_len = c_size_t(0)
        rc = self.lib.bre_get_rule_metadata_table(
            self.handle,
            ctypes.byref(out_metadata),
            ctypes.byref(out_len),
        )
        if rc != BRE_OK:
            raise RuntimeError(f"Failed to load rule metadata table, error code: {rc}")
        metadata = {}
        try:
            for idx in range(out_len.value):
                record = out_metadata[idx]
                destinations = []
                if record.destinations and record.destinations_len > 0:
                    for item_idx in range(record.destinations_len):
                        item = record.destinations[item_idx]
                        destinations.append(item.decode("utf-8") if item else "")
                metadata[int(record.rule_index)] = destinations
        finally:
            self.lib.bre_free_rule_metadata_table(out_metadata, out_len.value)
        return metadata

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
            if rc != BRE_OK:
                raise RuntimeError(f"Failed to start MQTT, error code: {rc}")
            self._mqtt_started = True
        self._attach_notify_reader()

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
        username = cfg["username"]
        return self.lib.bre_start_mqtt(
            self.handle,
            cfg["host"].encode("utf-8"),
            cfg["port"],
            cfg["node_id"].encode("utf-8") if cfg["node_id"] else None,
            cfg["client_count"],
            username.encode("utf-8") if username else None,
            cfg["password"].encode("utf-8") if cfg["password"] else None,
            cfg["clean_start"],
            cfg["session_expiry_interval"],
            cfg["group_name"].encode("utf-8"),
            cfg["ordered"],
            cfg["ordered_prefix"].encode("utf-8"),
            cfg["keep_alive_secs"],
            cfg["multi_nci"],
        )

    def _attach_notify_reader(self):
        if self._reader_attached:
            return
        if self._loop is None:
            raise RuntimeError("Event loop is not initialized")
        self._notify_fd = self.lib.bre_get_notify_fd(self.handle)
        if self._notify_fd == BRE_ERR_INVALID_ARGUMENT:
            raise RuntimeError("Failed to acquire notify fd from engine")
        self._loop.add_reader(self._notify_fd, self._on_notify_fd_ready)
        self._reader_attached = True
        self._loop.call_soon(self._drain_eval_results)

    def _detach_notify_reader(self):
        if self._reader_attached and self._loop is not None and self._notify_fd >= 0:
            self._loop.remove_reader(self._notify_fd)
        self._reader_attached = False

    def _on_notify_fd_ready(self):
        if self._notify_fd < 0:
            return
        try:
            while True:
                try:
                    data = os.read(self._notify_fd, 4096)
                except BlockingIOError:
                    break
                except OSError:
                    break
                if not data:
                    break
                if len(data) < 4096:
                    break
            self._drain_eval_results()
        except Exception:
            return

    def _drain_eval_results(self):
        if not self.handle:
            return
        out_results = _PackedEvalResults()
        rc = self.lib.bre_poll_eval_results_packed(
            self.handle,
            0,
            ctypes.byref(out_results),
        )
        if rc != BRE_POLL_RESULT_READY or out_results.len == 0:
            return
        try:
            payload_blob = (
                ctypes.string_at(out_results.payload_data, out_results.payload_data_len)
                if out_results.payload_data and out_results.payload_data_len > 0
                else b""
            )
            for idx in range(out_results.len):
                rule_index = int(out_results.rule_indices[idx])
                payload_offset = int(out_results.payload_offsets[idx])
                payload_length = int(out_results.payload_lengths[idx])
                payload = payload_blob[payload_offset : payload_offset + payload_length]
                destinations = self._rule_metadata.get(rule_index, [])
                if self._queue is not None:
                    self._queue.put_nowait((rule_index, payload, destinations))
        finally:
            self.lib.bre_free_packed_eval_results(ctypes.byref(out_results))

        if out_results.len > 0 and self._loop is not None:
            self._loop.call_soon(self._drain_eval_results)

    async def stop(self):
        self._mqtt_started = False
        self._detach_notify_reader()
        if self._queue is not None:
            await self._queue.put(None)

    def metrics(self):
        snapshot = _MetricsSnapshot()
        rc = self.lib.bre_metrics_snapshot(
            self.handle,
            ctypes.byref(snapshot),
        )
        if rc != BRE_OK:
            return None
        return {
            "ingress_message_count": snapshot.ingress_message_count,
            "core_queue_depth": snapshot.core_queue_depth,
            "core_queue_depth_max": snapshot.core_queue_depth_max,
            "core_queue_drop_count": snapshot.core_queue_drop_count,
            "ffi_queue_depth": snapshot.ffi_queue_depth,
            "ffi_queue_depth_max": snapshot.ffi_queue_depth_max,
            "ffi_queue_drop_count": snapshot.ffi_queue_drop_count,
            "message_pipeline_count": snapshot.message_pipeline_count,
            "message_pipeline_total_nanos": snapshot.message_pipeline_total_nanos,
            "message_pipeline_max_nanos": snapshot.message_pipeline_max_nanos,
            "eval_count": snapshot.eval_count,
            "eval_error_count": snapshot.eval_error_count,
            "exec_count": snapshot.exec_count,
            "exec_total_nanos": snapshot.exec_total_nanos,
            "exec_max_nanos": snapshot.exec_max_nanos,
        }

    def close(self):
        self.lib.bre_set_log_callback(None, None, 3)
        self._mqtt_started = False
        self._detach_notify_reader()
        if self.handle:
            self.lib.bre_destroy(self.handle)
            self.handle = None
