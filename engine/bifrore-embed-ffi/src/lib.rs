use bifrore_embed_core::message::Message;
use bifrore_embed_core::mqtt::{start_mqtt, MessageHandler, MqttAdapterHandle, MqttConfig};
use bifrore_embed_core::runtime::RuleEngine;
use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::{Mutex, Once, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BifroLogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

pub type BifroLogCallback =
    extern "C" fn(
        user_data: *mut c_void,
        level: c_int,
        target: *const c_char,
        message: *const c_char,
        timestamp_millis: u64,
        thread_id: *const c_char,
        module_path: *const c_char,
        file: *const c_char,
        line: u32,
    );

#[derive(Clone, Copy)]
struct LoggerCallbackState {
    callback: Option<BifroLogCallback>,
    user_data_addr: usize,
    min_level: BifroLogLevel,
}

impl Default for LoggerCallbackState {
    fn default() -> Self {
        Self {
            callback: None,
            user_data_addr: 0,
            min_level: BifroLogLevel::Info,
        }
    }
}

fn logger_state() -> &'static RwLock<LoggerCallbackState> {
    static STATE: OnceLock<RwLock<LoggerCallbackState>> = OnceLock::new();
    STATE.get_or_init(|| RwLock::new(LoggerCallbackState::default()))
}

struct FfiLogger;

impl FfiLogger {
    fn enabled_for(record_level: log::Level, min_level: BifroLogLevel) -> bool {
        match min_level {
            BifroLogLevel::Error => matches!(record_level, log::Level::Error),
            BifroLogLevel::Warn => matches!(record_level, log::Level::Error | log::Level::Warn),
            BifroLogLevel::Info => {
                matches!(record_level, log::Level::Error | log::Level::Warn | log::Level::Info)
            }
            BifroLogLevel::Debug => !matches!(record_level, log::Level::Trace),
            BifroLogLevel::Trace => true,
        }
    }
}

impl log::Log for FfiLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        let state = logger_state().read().unwrap();
        Self::enabled_for(metadata.level(), state.min_level)
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let state = *logger_state().read().unwrap();
        if let Some(callback) = state.callback {
            let target = CString::new(record.target().replace('\0', "\\0"))
                .unwrap_or_else(|_| CString::new("bifrore").unwrap());
            let message = CString::new(format!("{}", record.args()).replace('\0', "\\0"))
                .unwrap_or_else(|_| CString::new("log message encoding error").unwrap());
            let thread_id = CString::new(format!("{:?}", std::thread::current().id()))
                .unwrap_or_else(|_| CString::new("unknown-thread").unwrap());
            let module_path = record
                .module_path()
                .map(|value| CString::new(value.replace('\0', "\\0")).unwrap_or_else(|_| CString::new("module-encoding-error").unwrap()));
            let file = record
                .file()
                .map(|value| CString::new(value.replace('\0', "\\0")).unwrap_or_else(|_| CString::new("file-encoding-error").unwrap()));
            let level = match record.level() {
                log::Level::Error => BifroLogLevel::Error as c_int,
                log::Level::Warn => BifroLogLevel::Warn as c_int,
                log::Level::Info => BifroLogLevel::Info as c_int,
                log::Level::Debug => BifroLogLevel::Debug as c_int,
                log::Level::Trace => BifroLogLevel::Trace as c_int,
            };
            let user_data_ptr = state.user_data_addr as *mut c_void;
            let timestamp_millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            callback(
                user_data_ptr,
                level,
                target.as_ptr(),
                message.as_ptr(),
                timestamp_millis,
                thread_id.as_ptr(),
                module_path.as_ref().map_or(ptr::null(), |value| value.as_ptr()),
                file.as_ref().map_or(ptr::null(), |value| value.as_ptr()),
                record.line().unwrap_or(0),
            );
        } else {
            eprintln!("[{}][{}] {}", record.level(), record.target(), record.args());
        }
    }

    fn flush(&self) {}
}

fn ensure_logger_initialized() {
    static INIT: Once = Once::new();
    static LOGGER: FfiLogger = FfiLogger;
    INIT.call_once(|| {
        if log::set_logger(&LOGGER).is_ok() {
            log::set_max_level(log::LevelFilter::Trace);
        }
    });
}

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
    ensure_logger_initialized();
    let engine = BifroEngine {
        inner: Mutex::new(RuleEngine::new()),
        adapter: None,
    };
    log::info!("Bifro engine created");
    Box::into_raw(Box::new(engine))
}

#[no_mangle]
pub extern "C" fn bre_destroy(engine: *mut BifroEngine) {
    if engine.is_null() {
        return;
    }
    log::info!("Bifro engine destroyed");
    unsafe {
        drop(Box::from_raw(engine));
    }
}

#[no_mangle]
pub extern "C" fn bre_set_log_callback(
    callback: Option<BifroLogCallback>,
    user_data: *mut c_void,
    min_level: c_int,
) -> c_int {
    ensure_logger_initialized();
    let min_level = match min_level {
        1 => BifroLogLevel::Error,
        2 => BifroLogLevel::Warn,
        3 => BifroLogLevel::Info,
        4 => BifroLogLevel::Debug,
        5 => BifroLogLevel::Trace,
        _ => return -1,
    };

    let mut state = logger_state().write().unwrap();
    state.callback = callback;
    state.user_data_addr = user_data as usize;
    state.min_level = min_level;
    log::info!("log callback updated; level={}", min_level as c_int);
    0
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
    log::info!("loading rules from path={}", path);
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
    log::debug!("evaluating message topic={} payload_len={}", topic, payload_len);

    let engine = unsafe { &mut *engine };
    let guard = engine.inner.lock().unwrap();
    let results = guard.evaluate(&message);
    log::debug!("evaluation produced {} result(s)", results.len());

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
    log::info!(
        "starting MQTT from FFI host={} port={} client_id={}",
        config.host,
        config.port,
        config.client_id
    );

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
        log::info!("stopping MQTT adapter from FFI");
        match adapter.stop() {
            Ok(_) => 0,
            Err(_) => -2,
        }
    } else {
        0
    }
}
