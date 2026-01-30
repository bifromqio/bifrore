import ctypes
import json
from ctypes import c_char_p, c_void_p, c_int, c_uint16, c_uint32, c_size_t, c_bool


class BifroRE:
    def __init__(self, lib_path):
        self.lib = ctypes.cdll.LoadLibrary(lib_path)
        self._setup_signatures()
        self.handle = self.lib.bre_create()
        self._callback = None
        self._callback_c = None

    def _setup_signatures(self):
        self.lib.bre_create.restype = c_void_p
        self.lib.bre_destroy.argtypes = [c_void_p]
        self.lib.bre_load_rules_from_json.argtypes = [c_void_p, c_char_p]
        self.lib.bre_load_rules_from_json.restype = c_int
        self.lib.bre_eval.argtypes = [
            c_void_p, c_char_p, ctypes.POINTER(ctypes.c_ubyte), c_size_t, c_void_p, c_void_p
        ]
        self.lib.bre_eval.restype = c_int
        self.lib.bre_start_mqtt.argtypes = [
            c_void_p,
            c_char_p,
            c_uint16,
            c_char_p,
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

    def load_rules(self, path):
        return self.lib.bre_load_rules_from_json(self.handle, path.encode("utf-8"))

    def on_message(self, handler):
        CALLBACK = ctypes.CFUNCTYPE(
            None,
            c_void_p,
            c_char_p,
            ctypes.POINTER(ctypes.c_ubyte),
            c_size_t,
            c_char_p,
        )

        def _callback(user_data, rule_id, payload, payload_len, destinations_json):
            payload_bytes = bytes(payload[: payload_len])
            destinations = json.loads(destinations_json.decode("utf-8"))
            handler(rule_id.decode("utf-8"), payload_bytes, destinations)

        self._callback = handler
        self._callback_c = CALLBACK(_callback)

    def eval(self, topic, payload):
        if self._callback_c is None:
            raise RuntimeError("on_message handler not set")
        buf = (ctypes.c_ubyte * len(payload)).from_buffer_copy(payload)
        return self.lib.bre_eval(
            self.handle,
            topic.encode("utf-8"),
            buf,
            len(payload),
            self._callback_c,
            None,
        )

    def start_mqtt(
        self,
        host,
        port,
        client_id,
        group_name,
        username=None,
        password=None,
        clean_start=True,
        session_expiry_interval=3600,
        ordered=False,
        ordered_prefix="",
        keep_alive_secs=30,
    ):
        if self._callback_c is None:
            raise RuntimeError("on_message handler not set")
        return self.lib.bre_start_mqtt(
            self.handle,
            host.encode("utf-8"),
            port,
            client_id.encode("utf-8"),
            username.encode("utf-8") if username else None,
            password.encode("utf-8") if password else None,
            clean_start,
            session_expiry_interval,
            group_name.encode("utf-8"),
            ordered,
            ordered_prefix.encode("utf-8"),
            keep_alive_secs,
            self._callback_c,
            None,
        )

    def stop_mqtt(self):
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
        if self.handle:
            self.lib.bre_destroy(self.handle)
            self.handle = None


if __name__ == "__main__":
    # Example usage (adjust library path)
    engine = BifroRE("./libbifrore_embed.dylib")
    engine.on_message(lambda rule_id, payload, destinations: print(rule_id, destinations))
    engine.load_rules("./rule.json")
    engine.eval("data", b"{\"temp\": 30}")
    engine.close()
