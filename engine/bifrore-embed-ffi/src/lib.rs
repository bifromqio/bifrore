use bifrore_embed_core::message::Message;
use bifrore_embed_core::mqtt::{
    start_mqtt, IncomingDelivery, MessageHandler, MqttAdapterHandle, MqttConfig,
};
use bifrore_embed_core::payload::PayloadFormat;
use bifrore_embed_core::runtime::RuleEngine;
use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once, OnceLock, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_POLL_BATCH_SIZE: usize = 256;

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

fn create_notify_pipe() -> Option<(c_int, c_int)> {
    let mut fds = [0; 2];
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    for fd in &fds {
        let flags = unsafe { libc::fcntl(*fd, libc::F_GETFL) };
        if flags >= 0 {
            unsafe {
                libc::fcntl(*fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
        }
        let fd_flags = unsafe { libc::fcntl(*fd, libc::F_GETFD) };
        if fd_flags >= 0 {
            unsafe {
                libc::fcntl(*fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC);
            }
        }
    }
    Some((fds[0], fds[1]))
}

fn notify_eval_ready(fd: c_int) {
    if fd < 0 {
        return;
    }
    let byte: [u8; 1] = [1];
    let rc = unsafe { libc::write(fd, byte.as_ptr().cast(), 1) };
    if rc < 0 {
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default();
        if errno != libc::EAGAIN && errno != libc::EWOULDBLOCK {
            log::debug!("notify pipe write failed errno={}", errno);
        }
    }
}

fn maybe_notify_eval_ready(fd: c_int, pending: &AtomicBool) {
    if pending.swap(true, Ordering::AcqRel) {
        return;
    }
    notify_eval_ready(fd);
}

fn close_notify_pipe(fd: c_int) {
    if fd >= 0 {
        unsafe {
            libc::close(fd);
        }
    }
}

#[repr(C)]
pub struct BifroRE {
    inner: Mutex<RuleEngine>,
    adapter: Option<MqttAdapterHandle>,
    core_queue_tx: Option<flume::Sender<IncomingDelivery>>,
    core_worker: Option<JoinHandle<()>>,
    eval_result_tx: Option<flume::Sender<EvalResultRecord>>,
    eval_result_rx: Option<flume::Receiver<EvalResultRecord>>,
    notify_read_fd: c_int,
    notify_write_fd: c_int,
    notify_pending: AtomicBool,
}

pub type BifroEvalCallback = extern "C" fn(
    user_data: *mut c_void,
    rule_id: *const c_char,
    payload: *const u8,
    payload_len: size_t,
    destinations_json: *const c_char,
);

#[derive(Debug)]
struct EvalResultRecord {
    rule_id: String,
    payload: Vec<u8>,
    destinations_json: String,
}

#[repr(C)]
pub struct BifroEvalResult {
    pub rule_id: *mut c_char,
    pub payload: *mut u8,
    pub payload_len: size_t,
    pub destinations_json: *mut c_char,
}

impl Default for BifroEvalResult {
    fn default() -> Self {
        Self {
            rule_id: ptr::null_mut(),
            payload: ptr::null_mut(),
            payload_len: 0,
            destinations_json: ptr::null_mut(),
        }
    }
}

fn to_ffi_eval_result(record: EvalResultRecord) -> BifroEvalResult {
    let rule_id = CString::new(record.rule_id.replace('\0', "\\0"))
        .unwrap_or_else(|_| CString::new("").unwrap())
        .into_raw();
    let destinations_json = CString::new(record.destinations_json.replace('\0', "\\0"))
        .unwrap_or_else(|_| CString::new("[]").unwrap())
        .into_raw();
    let payload_len = record.payload.len();
    let payload = if payload_len == 0 {
        ptr::null_mut()
    } else {
        let mut payload = record.payload.into_boxed_slice();
        let payload_ptr = payload.as_mut_ptr();
        std::mem::forget(payload);
        payload_ptr
    };
    BifroEvalResult {
        rule_id,
        payload,
        payload_len,
        destinations_json,
    }
}

fn free_eval_result_inner(result_ref: &mut BifroEvalResult) {
    unsafe {
        if !result_ref.rule_id.is_null() {
            let _ = CString::from_raw(result_ref.rule_id);
            result_ref.rule_id = ptr::null_mut();
        }
        if !result_ref.destinations_json.is_null() {
            let _ = CString::from_raw(result_ref.destinations_json);
            result_ref.destinations_json = ptr::null_mut();
        }
        if !result_ref.payload.is_null() && result_ref.payload_len > 0 {
            let _ = Vec::from_raw_parts(
                result_ref.payload,
                result_ref.payload_len,
                result_ref.payload_len,
            );
            result_ref.payload = ptr::null_mut();
            result_ref.payload_len = 0;
        }
    }
}

#[no_mangle]
pub extern "C" fn bre_create_with_config(config_path: *const c_char) -> *mut BifroRE {
    bre_create_with_config_and_payload_format(config_path, BifroREPayloadFormat::Json as c_int)
}

#[no_mangle]
pub extern "C" fn bre_create_with_config_and_payload_format(
    config_path: *const c_char,
    payload_format: c_int,
) -> *mut BifroRE {
    ensure_logger_initialized();
    if config_path.is_null() {
        return ptr::null_mut();
    }
    let Some(payload_format) = PayloadFormat::from_ffi_code(payload_format) else {
        return ptr::null_mut();
    };

    let config_path = unsafe { CStr::from_ptr(config_path) };
    let Ok(config_path) = config_path.to_str() else {
        return ptr::null_mut();
    };

    let mut rule_engine = RuleEngine::with_payload_format(payload_format);
    if rule_engine.load_rules_from_json(config_path).is_err() {
        return ptr::null_mut();
    }

    let (notify_read_fd, notify_write_fd) = create_notify_pipe().unwrap_or((-1, -1));
    let engine = BifroRE {
        inner: Mutex::new(rule_engine),
        adapter: None,
        core_queue_tx: None,
        core_worker: None,
        eval_result_tx: None,
        eval_result_rx: None,
        notify_read_fd,
        notify_write_fd,
        notify_pending: AtomicBool::new(false),
    };
    log::info!(
        "BifroRE created with config path={} payload_format={:?}",
        config_path,
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
        stop_core_worker(&mut boxed);
        close_notify_pipe(boxed.notify_read_fd);
        close_notify_pipe(boxed.notify_write_fd);
        boxed.notify_read_fd = -1;
        boxed.notify_write_fd = -1;
        log::info!("BifroRE destroyed");
        drop(boxed);
    }
}

#[no_mangle]
pub extern "C" fn bre_get_notify_fd(engine: *const BifroRE) -> c_int {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &*engine };
    engine.notify_read_fd
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
pub extern "C" fn bre_set_eval_parallel_threshold(
    engine: *mut BifroRE,
    threshold: u32,
) -> c_int {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &mut *engine };
    let mut guard = engine.inner.lock().unwrap();
    if threshold == 0 {
        guard.set_eval_parallel_threshold(None);
    } else {
        guard.set_eval_parallel_threshold(Some(threshold as usize));
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
    multi_nci: bool,
    _callback: Option<BifroEvalCallback>,
    _user_data: *mut c_void,
) -> c_int {
    if engine.is_null()
        || host.is_null()
        || client_prefix.is_null()
        || group_name.is_null()
        || ordered_prefix.is_null()
    {
        return -1;
    }
    let engine_ref = unsafe { &mut *engine };
    if engine_ref.adapter.is_some() {
        return -5;
    }

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
        eval_threads: 1,
        queue_capacity: 4096,
        username,
        password,
        clean_start,
        session_expiry_interval,
        group_name,
        ordered,
        ordered_prefix,
        keep_alive_secs,
        multi_nci,
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
    let (core_queue_tx, core_queue_rx) =
        flume::bounded::<IncomingDelivery>(config.queue_capacity.max(1) as usize);
    let (eval_result_tx, eval_result_rx) =
        flume::bounded::<EvalResultRecord>(config.queue_capacity.max(1) as usize);
    let eval_result_tx_for_worker = eval_result_tx.clone();
    let notify_write_fd = engine_ref.notify_write_fd;
    let core_worker = std::thread::spawn(move || {
        let engine_ptr = engine_addr as *mut BifroRE;
        while let Ok(delivery) = core_queue_rx.recv() {
            let message: &Message = &delivery.message;
            let engine = unsafe { &*engine_ptr };
            let mut guard = engine.inner.lock().unwrap();
            let results = guard.evaluate(message);
            drop(guard);
            delivery.ack();
            let mut enqueued_any = false;
            for result in results {
                let destinations_json = serde_json::to_string(&result.destinations)
                    .unwrap_or_else(|_| "[]".to_string());
                let eval_result = EvalResultRecord {
                    rule_id: result.rule_id,
                    payload: result.message.payload,
                    destinations_json,
                };
                if eval_result_tx_for_worker.send(eval_result).is_err() {
                    log::warn!("dropping eval result because poll queue is closed");
                    break;
                }
                enqueued_any = true;
            }
            if enqueued_any {
                maybe_notify_eval_ready(notify_write_fd, &engine.notify_pending);
            }
        }
    });

    let core_queue_tx_for_handler = core_queue_tx.clone();
    let handler: MessageHandler = std::sync::Arc::new(move |delivery: IncomingDelivery| {
        if core_queue_tx_for_handler.send(delivery).is_err() {
            log::warn!("dropping incoming message because core queue is closed");
        }
    });

    let topics = {
        let guard = engine_ref.inner.lock().unwrap();
        guard.topic_filters()
    };

    let adapter = match start_mqtt(config, topics, handler) {
        Ok(adapter) => adapter,
        Err(_) => {
            drop(core_queue_tx);
            drop(eval_result_tx);
            let _ = core_worker.join();
            return -4;
        }
    };
    engine_ref.adapter = Some(adapter);
    engine_ref.core_queue_tx = Some(core_queue_tx);
    engine_ref.core_worker = Some(core_worker);
    engine_ref.eval_result_tx = Some(eval_result_tx);
    engine_ref.eval_result_rx = Some(eval_result_rx);
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
            Ok(_) => {
                stop_core_worker(engine);
                0
            }
            Err(_) => -2,
        }
    } else {
        stop_core_worker(engine);
        0
    }
}

fn stop_core_worker(engine: &mut BifroRE) {
    engine.core_queue_tx.take();
    engine.eval_result_tx.take();
    if let Some(worker) = engine.core_worker.take() {
        let _ = worker.join();
    }
    engine.eval_result_rx.take();
}

#[no_mangle]
pub extern "C" fn bre_poll_eval_results_batch(
    engine: *mut BifroRE,
    timeout_millis: u32,
    out_results: *mut *mut BifroEvalResult,
    out_len: *mut size_t,
) -> c_int {
    if engine.is_null() || out_results.is_null() || out_len.is_null() {
        return -1;
    }
    let engine = unsafe { &mut *engine };
    let Some(receiver) = engine.eval_result_rx.as_ref() else {
        return -2;
    };
    let max_results = DEFAULT_POLL_BATCH_SIZE;

    let first_record = if timeout_millis == 0 {
        match receiver.try_recv() {
            Ok(value) => value,
            Err(flume::TryRecvError::Empty) => {
                engine.notify_pending.store(false, Ordering::Release);
                if !receiver.is_empty() {
                    maybe_notify_eval_ready(engine.notify_write_fd, &engine.notify_pending);
                }
                return 0;
            }
            Err(flume::TryRecvError::Disconnected) => return -3,
        }
    } else if timeout_millis == u32::MAX {
        match receiver.recv() {
            Ok(value) => value,
            Err(_) => return -3,
        }
    } else {
        match receiver.recv_timeout(Duration::from_millis(timeout_millis as u64)) {
            Ok(value) => value,
            Err(flume::RecvTimeoutError::Timeout) => return 0,
            Err(flume::RecvTimeoutError::Disconnected) => return -3,
        }
    };

    let mut results = Vec::with_capacity(max_results as usize);
    results.push(to_ffi_eval_result(first_record));

    while results.len() < max_results as usize {
        match receiver.try_recv() {
            Ok(record) => results.push(to_ffi_eval_result(record)),
            Err(flume::TryRecvError::Empty) => break,
            Err(flume::TryRecvError::Disconnected) => break,
        }
    }

    if receiver.is_empty() {
        engine.notify_pending.store(false, Ordering::Release);
    }

    let mut boxed = results.into_boxed_slice();
    let len = boxed.len();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    unsafe {
        *out_results = ptr;
        *out_len = len;
    }
    1
}

#[no_mangle]
pub extern "C" fn bre_free_eval_result(result: *mut BifroEvalResult) {
    if result.is_null() {
        return;
    }
    let result_ref = unsafe { &mut *result };
    free_eval_result_inner(result_ref);
}

#[no_mangle]
pub extern "C" fn bre_free_eval_results_batch(results: *mut BifroEvalResult, len: size_t) {
    if results.is_null() || len == 0 {
        return;
    }
    unsafe {
        let mut records = Vec::from_raw_parts(results, len, len);
        for result in &mut records {
            free_eval_result_inner(result);
        }
    }
}
