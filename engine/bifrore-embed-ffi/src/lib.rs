use bifrore_embed_core::message::Message;
use bifrore_embed_core::mqtt::{start_mqtt, MessageHandler, MqttAdapterHandle, MqttConfig};
use bifrore_embed_core::payload::PayloadFormat;
use bifrore_embed_core::runtime::RuleEngine;
use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::{Mutex, Once, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BifroRELogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BifroREPayloadFormat {
    Json = 1,
    Protobuf = 2,
}

pub type BifroRELogCallback =
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
    callback: Option<BifroRELogCallback>,
    user_data_addr: usize,
    min_level: BifroRELogLevel,
}

impl Default for LoggerCallbackState {
    fn default() -> Self {
        Self {
            callback: None,
            user_data_addr: 0,
            min_level: BifroRELogLevel::Info,
        }
    }
}

fn logger_state() -> &'static RwLock<LoggerCallbackState> {
    static STATE: OnceLock<RwLock<LoggerCallbackState>> = OnceLock::new();
    STATE.get_or_init(|| RwLock::new(LoggerCallbackState::default()))
}

struct FfiLogger;

impl FfiLogger {
    fn enabled_for(record_level: log::Level, min_level: BifroRELogLevel) -> bool {
        match min_level {
            BifroRELogLevel::Error => matches!(record_level, log::Level::Error),
            BifroRELogLevel::Warn => matches!(record_level, log::Level::Error | log::Level::Warn),
            BifroRELogLevel::Info => {
                matches!(record_level, log::Level::Error | log::Level::Warn | log::Level::Info)
            }
            BifroRELogLevel::Debug => !matches!(record_level, log::Level::Trace),
            BifroRELogLevel::Trace => true,
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
                log::Level::Error => BifroRELogLevel::Error as c_int,
                log::Level::Warn => BifroRELogLevel::Warn as c_int,
                log::Level::Info => BifroRELogLevel::Info as c_int,
                log::Level::Debug => BifroRELogLevel::Debug as c_int,
                log::Level::Trace => BifroRELogLevel::Trace as c_int,
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

fn generate_default_node_id() -> String {
    let pid = std::process::id();
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    format!("node_{}_{}", pid, millis)
}

#[repr(C)]
pub struct BifroRE {
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
pub extern "C" fn bre_create() -> *mut BifroRE {
    ensure_logger_initialized();
    let engine = BifroRE {
        inner: Mutex::new(RuleEngine::with_payload_format(PayloadFormat::Json)),
        adapter: None,
    };
    log::info!("Bifro engine created");
    Box::into_raw(Box::new(engine))
}

#[no_mangle]
pub extern "C" fn bre_create_with_payload_format(payload_format: c_int) -> *mut BifroRE {
    ensure_logger_initialized();
    let Some(payload_format) = PayloadFormat::from_ffi_code(payload_format) else {
        return ptr::null_mut();
    };
    let engine = BifroRE {
        inner: Mutex::new(RuleEngine::with_payload_format(payload_format)),
        adapter: None,
    };
    log::info!("Bifro engine created with payload_format={:?}", payload_format);
    Box::into_raw(Box::new(engine))
}

#[no_mangle]
pub extern "C" fn bre_create_with_rules(path: *const c_char) -> *mut BifroRE {
    bre_create_with_rules_and_payload_format(path, BifroREPayloadFormat::Json as c_int)
}

#[no_mangle]
pub extern "C" fn bre_create_with_rules_and_payload_format(
    path: *const c_char,
    payload_format: c_int,
) -> *mut BifroRE {
    ensure_logger_initialized();
    if path.is_null() {
        return ptr::null_mut();
    }
    let Some(payload_format) = PayloadFormat::from_ffi_code(payload_format) else {
        return ptr::null_mut();
    };

    let path = unsafe { CStr::from_ptr(path) };
    let Ok(path) = path.to_str() else {
        return ptr::null_mut();
    };

    let mut rule_engine = RuleEngine::with_payload_format(payload_format);
    if rule_engine.load_rules_from_json(path).is_err() {
        return ptr::null_mut();
    }

    let engine = BifroRE {
        inner: Mutex::new(rule_engine),
        adapter: None,
    };
    log::info!(
        "BifroRE created with rules path={} payload_format={:?}",
        path,
        payload_format
    );
    Box::into_raw(Box::new(engine))
}

#[no_mangle]
pub extern "C" fn bre_destroy(engine: *mut BifroRE) {
    if engine.is_null() {
        return;
    }
    unsafe {
        let mut boxed = Box::from_raw(engine);
        if let Some(adapter) = boxed.adapter.take() {
            let _ = adapter.stop();
            log::info!("BifroRE destroy requested MQTT stop");
        }
        log::info!("BifroRE destroyed");
        drop(boxed);
    }
}

#[no_mangle]
pub extern "C" fn bre_set_log_callback(
    callback: Option<BifroRELogCallback>,
    user_data: *mut c_void,
    min_level: c_int,
) -> c_int {
    ensure_logger_initialized();
    let min_level = match min_level {
        1 => BifroRELogLevel::Error,
        2 => BifroRELogLevel::Warn,
        3 => BifroRELogLevel::Info,
        4 => BifroRELogLevel::Debug,
        5 => BifroRELogLevel::Trace,
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
pub extern "C" fn bre_metrics_snapshot(
    engine: *const BifroRE,
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
    engine: *mut BifroRE,
    host: *const c_char,
    port: u16,
    client_prefix: *const c_char,
    node_id: *const c_char,
    client_count: u16,
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
        || client_prefix.is_null()
        || group_name.is_null()
        || ordered_prefix.is_null()
    {
        return -1;
    }
    let Some(callback) = callback else { return -2 };

    let host = unsafe { CStr::from_ptr(host) };
    let client_prefix = unsafe { CStr::from_ptr(client_prefix) };
    let group_name = unsafe { CStr::from_ptr(group_name) };
    let ordered_prefix = unsafe { CStr::from_ptr(ordered_prefix) };
    let host = match host.to_str() {
        Ok(val) => val.to_string(),
        Err(_) => return -3,
    };
    let client_prefix = match client_prefix.to_str() {
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
    let node_id = if node_id.is_null() {
        generate_default_node_id()
    } else {
        let parsed = unsafe { CStr::from_ptr(node_id) }
            .to_str()
            .ok()
            .map(|value| value.trim().to_string())
            .unwrap_or_default();
        if parsed.is_empty() {
            generate_default_node_id()
        } else {
            parsed
        }
    };

    let config = MqttConfig {
        host,
        port,
        client_prefix,
        node_id,
        client_count,
        io_threads: 2,
        eval_threads: std::thread::available_parallelism()
            .map(|count| count.get().clamp(1, 4) as u16)
            .unwrap_or(2),
        queue_capacity: 4096,
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
        "Starting MQTT from FFI host={} port={} client_prefix={} node_id={} clients={}",
        config.host,
        config.port,
        config.client_prefix,
        config.node_id,
        if config.client_count == 0 { 1 } else { config.client_count }
    );

    let engine_addr = engine as usize;
    let user_data_addr = user_data as usize;
    let handler: MessageHandler = std::sync::Arc::new(move |message: Message| {
        let engine_ptr = engine_addr as *mut BifroRE;
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
pub extern "C" fn bre_stop_mqtt(engine: *mut BifroRE) -> c_int {
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
