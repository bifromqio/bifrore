use bifrore_embed_core::message::Message;
use bifrore_embed_core::mqtt::{start_mqtt, MessageHandler, MqttAdapterHandle, MqttConfig};
use bifrore_embed_core::runtime::RuleEngine;
use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::Mutex;

#[repr(C)]
pub struct BifroEngine {
    inner: Mutex<RuleEngine>,
    adapter: Option<MqttAdapterHandle>,
}

pub type BifroEvalCallback = extern "C" fn(
    user_data: *mut c_void,
    rule_id: *const c_char,
    payload: *const u8,
    payload_len: size_t,
    destinations_json: *const c_char,
);

#[no_mangle]
pub extern "C" fn bre_create() -> *mut BifroEngine {
    let engine = BifroEngine {
        inner: Mutex::new(RuleEngine::new()),
        adapter: None,
    };
    Box::into_raw(Box::new(engine))
}

#[no_mangle]
pub extern "C" fn bre_destroy(engine: *mut BifroEngine) {
    if engine.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(engine));
    }
}

#[no_mangle]
pub extern "C" fn bre_load_rules_from_json(
    engine: *mut BifroEngine,
    path: *const c_char,
) -> c_int {
    if engine.is_null() || path.is_null() {
        return -1;
    }
    let path = unsafe { CStr::from_ptr(path) };
    let Ok(path) = path.to_str() else { return -2 };
    let engine = unsafe { &mut *engine };
    let mut guard = engine.inner.lock().unwrap();
    match guard.load_rules_from_json(path) {
        Ok(_) => 0,
        Err(_) => -3,
    }
}

#[no_mangle]
pub extern "C" fn bre_eval(
    engine: *mut BifroEngine,
    topic: *const c_char,
    payload: *const u8,
    payload_len: size_t,
    callback: Option<BifroEvalCallback>,
    user_data: *mut c_void,
) -> c_int {
    if engine.is_null() || topic.is_null() || payload.is_null() {
        return -1;
    }
    let Some(callback) = callback else { return -2 };

    let topic = unsafe { CStr::from_ptr(topic) };
    let Ok(topic) = topic.to_str() else { return -3 };

    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len as usize) };
    let message = Message::new(topic, payload.to_vec());

    let engine = unsafe { &mut *engine };
    let guard = engine.inner.lock().unwrap();
    let results = guard.evaluate(&message);

    for result in results {
        let rule_id = CString::new(result.rule_id).unwrap_or_else(|_| CString::new("").unwrap());
        let destinations_json = serde_json::to_string(&result.destinations)
            .unwrap_or_else(|_| "[]".to_string());
        let destinations_json = CString::new(destinations_json)
            .unwrap_or_else(|_| CString::new("[]").unwrap());

        callback(
            user_data,
            rule_id.as_ptr(),
            result.message.payload.as_ptr(),
            result.message.payload.len() as size_t,
            destinations_json.as_ptr(),
        );
    }
    0
}

#[no_mangle]
pub extern "C" fn bre_metrics_snapshot(
    engine: *const BifroEngine,
    eval_count: *mut u64,
    eval_error_count: *mut u64,
    eval_total_nanos: *mut u64,
    eval_max_nanos: *mut u64,
) -> c_int {
    if engine.is_null() {
        return -1;
    }
    let guard = unsafe { &*engine }.inner.lock().unwrap();
    let snapshot = guard.metrics().snapshot();
    unsafe {
        if !eval_count.is_null() {
            ptr::write(eval_count, snapshot.eval_count);
        }
        if !eval_error_count.is_null() {
            ptr::write(eval_error_count, snapshot.eval_error_count);
        }
        if !eval_total_nanos.is_null() {
            ptr::write(eval_total_nanos, snapshot.eval_total_nanos);
        }
        if !eval_max_nanos.is_null() {
            ptr::write(eval_max_nanos, snapshot.eval_max_nanos);
        }
    }
    0
}

#[no_mangle]
pub extern "C" fn bre_start_mqtt(
    engine: *mut BifroEngine,
    host: *const c_char,
    port: u16,
    client_id: *const c_char,
    username: *const c_char,
    password: *const c_char,
    clean_start: bool,
    session_expiry_interval: u32,
    group_name: *const c_char,
    ordered: bool,
    ordered_prefix: *const c_char,
    keep_alive_secs: u16,
    callback: Option<BifroEvalCallback>,
    user_data: *mut c_void,
) -> c_int {
    if engine.is_null()
        || host.is_null()
        || client_id.is_null()
        || group_name.is_null()
        || ordered_prefix.is_null()
    {
        return -1;
    }
    let Some(callback) = callback else { return -2 };

    let host = unsafe { CStr::from_ptr(host) };
    let client_id = unsafe { CStr::from_ptr(client_id) };
    let group_name = unsafe { CStr::from_ptr(group_name) };
    let ordered_prefix = unsafe { CStr::from_ptr(ordered_prefix) };
    let host = match host.to_str() {
        Ok(val) => val.to_string(),
        Err(_) => return -3,
    };
    let client_id = match client_id.to_str() {
        Ok(val) => val.to_string(),
        Err(_) => return -3,
    };
    let group_name = match group_name.to_str() {
        Ok(val) => val.to_string(),
        Err(_) => return -3,
    };
    let ordered_prefix = match ordered_prefix.to_str() {
        Ok(val) => val.to_string(),
        Err(_) => return -3,
    };

    let username = if username.is_null() {
        None
    } else {
        unsafe { CStr::from_ptr(username) }
            .to_str()
            .ok()
            .map(|v| v.to_string())
    };
    let password = if password.is_null() {
        None
    } else {
        unsafe { CStr::from_ptr(password) }
            .to_str()
            .ok()
            .map(|v| v.to_string())
    };

    let config = MqttConfig {
        host,
        port,
        client_id,
        username,
        password,
        clean_start,
        session_expiry_interval,
        group_name,
        ordered,
        ordered_prefix,
        keep_alive_secs,
    };

    let engine_addr = engine as usize;
    let user_data_addr = user_data as usize;
    let handler: MessageHandler = std::sync::Arc::new(move |message: Message| {
        let engine_ptr = engine_addr as *mut BifroEngine;
        let user_data_ptr = user_data_addr as *mut c_void;
        let engine = unsafe { &*engine_ptr };
        let guard = engine.inner.lock().unwrap();
        let results = guard.evaluate(&message);
        drop(guard);
        for result in results {
            let rule_id =
                CString::new(result.rule_id).unwrap_or_else(|_| CString::new("").unwrap());
            let destinations_json = serde_json::to_string(&result.destinations)
                .unwrap_or_else(|_| "[]".to_string());
            let destinations_json = CString::new(destinations_json)
                .unwrap_or_else(|_| CString::new("[]").unwrap());
            callback(
                user_data_ptr,
                rule_id.as_ptr(),
                result.message.payload.as_ptr(),
                result.message.payload.len() as size_t,
                destinations_json.as_ptr(),
            );
        }
    });

    let topics = {
        let engine_ref = unsafe { &mut *engine };
        let guard = engine_ref.inner.lock().unwrap();
        guard.topic_filters()
    };

    let adapter = match start_mqtt(config, topics, handler) {
        Ok(adapter) => adapter,
        Err(_) => return -4,
    };
    unsafe { &mut *engine }.adapter = Some(adapter);
    0
}

#[no_mangle]
pub extern "C" fn bre_stop_mqtt(engine: *mut BifroEngine) -> c_int {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &mut *engine };
    if let Some(adapter) = engine.adapter.take() {
        match adapter.stop() {
            Ok(_) => 0,
            Err(_) => -2,
        }
    } else {
        0
    }
}
