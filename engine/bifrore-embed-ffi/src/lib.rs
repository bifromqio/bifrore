use bifrore_embed_core::message::Message;
use bifrore_embed_core::metrics::{EvalMetricsSnapshot, LatencyMetricsSnapshot};
use bifrore_embed_core::mqtt::{
    start_mqtt, IncomingDelivery, MessageHandler, MqttAdapterHandle, MqttConfig,
};
use bifrore_embed_core::payload::PayloadFormat;
use bifrore_embed_core::runtime::{RuleEngine, RuleMetadata};
use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::fs;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once, OnceLock, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_POLL_BATCH_SIZE: usize = 256;
const DEFAULT_CLIENT_IDS_PATH: &str = "./client_ids";

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

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BifroRENotifyMode {
    Poll = 0,
    PushNotify = 1,
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

fn normalize_client_count(client_count: u16) -> usize {
    client_count.max(1) as usize
}

fn load_client_ids(path: &str) -> Option<Vec<String>> {
    let content = fs::read_to_string(path).ok()?;
    let values = content
        .lines()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    if values.is_empty() { None } else { Some(values) }
}

fn persist_client_ids(path: &str, client_ids: &[String]) -> Result<(), String> {
    if client_ids.is_empty() {
        return Ok(());
    }
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
    }
    fs::write(path_ref, format!("{}\n", client_ids.join("\n"))).map_err(|err| err.to_string())
}

fn resolve_client_ids(
    path: &str,
    node_id: &str,
    client_count: u16,
) -> Vec<String> {
    if let Some(values) = load_client_ids(path) {
        return values;
    }
    (0..normalize_client_count(client_count))
        .map(|index| format!("{}_{}", node_id, index))
        .collect()
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
    poll_batch_limit: usize,
    client_ids_path: String,
    active_client_ids: Vec<String>,
    ffi_metrics: FfiMetrics,
}

pub type BifroEvalCallback = extern "C" fn(
    user_data: *mut c_void,
    rule_index: u16,
    payload: *const u8,
    payload_len: size_t,
);

#[derive(Debug)]
struct EvalResultRecord {
    rule_index: u16,
    payload: Vec<u8>,
}

#[derive(Debug, Default)]
struct FfiMetrics {
    ingress_message_count: std::sync::atomic::AtomicU64,
    core_queue_depth: std::sync::atomic::AtomicU64,
    core_queue_depth_max: std::sync::atomic::AtomicU64,
    core_queue_drop_count: std::sync::atomic::AtomicU64,
    ffi_queue_depth: std::sync::atomic::AtomicU64,
    ffi_queue_depth_max: std::sync::atomic::AtomicU64,
    ffi_queue_drop_count: std::sync::atomic::AtomicU64,
}

impl FfiMetrics {
    fn record_ingress(&self) {
        self.ingress_message_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_core_queue_enqueued(&self) {
        let depth = self.core_queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        update_max_atomic(&self.core_queue_depth_max, depth);
    }

    fn record_core_queue_dequeued(&self) {
        self.core_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn record_core_queue_drop(&self) {
        self.core_queue_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_ffi_queue_enqueued(&self) {
        let depth = self.ffi_queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        update_max_atomic(&self.ffi_queue_depth_max, depth);
    }

    fn record_ffi_queue_dequeued(&self, count: u64) {
        if count > 0 {
            self.ffi_queue_depth.fetch_sub(count, Ordering::Relaxed);
        }
    }

    fn record_ffi_queue_drop(&self) {
        self.ffi_queue_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> FfiMetricsSnapshot {
        FfiMetricsSnapshot {
            ingress_message_count: self.ingress_message_count.load(Ordering::Relaxed),
            core_queue_depth: self.core_queue_depth.load(Ordering::Relaxed),
            core_queue_depth_max: self.core_queue_depth_max.load(Ordering::Relaxed),
            core_queue_drop_count: self.core_queue_drop_count.load(Ordering::Relaxed),
            ffi_queue_depth: self.ffi_queue_depth.load(Ordering::Relaxed),
            ffi_queue_depth_max: self.ffi_queue_depth_max.load(Ordering::Relaxed),
            ffi_queue_drop_count: self.ffi_queue_drop_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct FfiMetricsSnapshot {
    ingress_message_count: u64,
    core_queue_depth: u64,
    core_queue_depth_max: u64,
    core_queue_drop_count: u64,
    ffi_queue_depth: u64,
    ffi_queue_depth_max: u64,
    ffi_queue_drop_count: u64,
}

#[repr(C)]
pub struct BifroREMetricsSnapshot {
    pub ingress_message_count: u64,
    pub core_queue_depth: u64,
    pub core_queue_depth_max: u64,
    pub core_queue_drop_count: u64,
    pub ffi_queue_depth: u64,
    pub ffi_queue_depth_max: u64,
    pub ffi_queue_drop_count: u64,
    pub eval_count: u64,
    pub eval_error_count: u64,
    pub eval_total_total_nanos: u64,
    pub eval_total_max_nanos: u64,
    pub topic_match_count: u64,
    pub topic_match_total_nanos: u64,
    pub topic_match_max_nanos: u64,
    pub payload_decode_count: u64,
    pub payload_decode_total_nanos: u64,
    pub payload_decode_max_nanos: u64,
    pub msg_ir_build_count: u64,
    pub msg_ir_build_total_nanos: u64,
    pub msg_ir_build_max_nanos: u64,
    pub fast_where_count: u64,
    pub fast_where_total_nanos: u64,
    pub fast_where_max_nanos: u64,
    pub predicate_count: u64,
    pub predicate_total_nanos: u64,
    pub predicate_max_nanos: u64,
    pub projection_count: u64,
    pub projection_total_nanos: u64,
    pub projection_max_nanos: u64,
}

fn update_max_atomic(target: &std::sync::atomic::AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

fn write_latency_snapshot(
    count: &mut u64,
    total_nanos: &mut u64,
    max_nanos: &mut u64,
    snapshot: LatencyMetricsSnapshot,
) {
    *count = snapshot.count;
    *total_nanos = snapshot.total_nanos;
    *max_nanos = snapshot.max_nanos;
}

fn combine_metrics_snapshot(
    ffi: FfiMetricsSnapshot,
    eval: EvalMetricsSnapshot,
) -> BifroREMetricsSnapshot {
    let mut snapshot = BifroREMetricsSnapshot {
        ingress_message_count: ffi.ingress_message_count,
        core_queue_depth: ffi.core_queue_depth,
        core_queue_depth_max: ffi.core_queue_depth_max,
        core_queue_drop_count: ffi.core_queue_drop_count,
        ffi_queue_depth: ffi.ffi_queue_depth,
        ffi_queue_depth_max: ffi.ffi_queue_depth_max,
        ffi_queue_drop_count: ffi.ffi_queue_drop_count,
        eval_count: eval.eval_count,
        eval_error_count: eval.eval_error_count,
        eval_total_total_nanos: eval.eval_total.total_nanos,
        eval_total_max_nanos: eval.eval_total.max_nanos,
        topic_match_count: 0,
        topic_match_total_nanos: 0,
        topic_match_max_nanos: 0,
        payload_decode_count: 0,
        payload_decode_total_nanos: 0,
        payload_decode_max_nanos: 0,
        msg_ir_build_count: 0,
        msg_ir_build_total_nanos: 0,
        msg_ir_build_max_nanos: 0,
        fast_where_count: 0,
        fast_where_total_nanos: 0,
        fast_where_max_nanos: 0,
        predicate_count: 0,
        predicate_total_nanos: 0,
        predicate_max_nanos: 0,
        projection_count: 0,
        projection_total_nanos: 0,
        projection_max_nanos: 0,
    };
    write_latency_snapshot(
        &mut snapshot.topic_match_count,
        &mut snapshot.topic_match_total_nanos,
        &mut snapshot.topic_match_max_nanos,
        eval.topic_match,
    );
    write_latency_snapshot(
        &mut snapshot.payload_decode_count,
        &mut snapshot.payload_decode_total_nanos,
        &mut snapshot.payload_decode_max_nanos,
        eval.payload_decode,
    );
    write_latency_snapshot(
        &mut snapshot.msg_ir_build_count,
        &mut snapshot.msg_ir_build_total_nanos,
        &mut snapshot.msg_ir_build_max_nanos,
        eval.msg_ir_build,
    );
    write_latency_snapshot(
        &mut snapshot.fast_where_count,
        &mut snapshot.fast_where_total_nanos,
        &mut snapshot.fast_where_max_nanos,
        eval.fast_where,
    );
    write_latency_snapshot(
        &mut snapshot.predicate_count,
        &mut snapshot.predicate_total_nanos,
        &mut snapshot.predicate_max_nanos,
        eval.predicate,
    );
    write_latency_snapshot(
        &mut snapshot.projection_count,
        &mut snapshot.projection_total_nanos,
        &mut snapshot.projection_max_nanos,
        eval.projection,
    );
    snapshot
}

#[repr(C)]
pub struct BifroPackedEvalResults {
    pub rule_indices: *mut u32,
    pub payload_offsets: *mut u32,
    pub payload_lengths: *mut u32,
    pub payload_data: *mut u8,
    pub len: size_t,
    pub payload_data_len: size_t,
}

#[repr(C)]
pub struct BifroRuleMetadata {
    pub rule_index: u16,
    pub destinations: *mut *mut c_char,
    pub destinations_len: size_t,
}

fn free_packed_eval_results_inner(results: &mut BifroPackedEvalResults) {
    unsafe {
        if !results.rule_indices.is_null() && results.len > 0 {
            let _ = Vec::from_raw_parts(results.rule_indices, results.len, results.len);
            results.rule_indices = ptr::null_mut();
        }
        if !results.payload_offsets.is_null() && results.len > 0 {
            let _ = Vec::from_raw_parts(results.payload_offsets, results.len, results.len);
            results.payload_offsets = ptr::null_mut();
        }
        if !results.payload_lengths.is_null() && results.len > 0 {
            let _ = Vec::from_raw_parts(results.payload_lengths, results.len, results.len);
            results.payload_lengths = ptr::null_mut();
        }
        if !results.payload_data.is_null() && results.payload_data_len > 0 {
            let _ = Vec::from_raw_parts(
                results.payload_data,
                results.payload_data_len,
                results.payload_data_len,
            );
            results.payload_data = ptr::null_mut();
        }
        results.len = 0;
        results.payload_data_len = 0;
    }
}

fn to_ffi_rule_metadata(metadata: RuleMetadata) -> BifroRuleMetadata {
    let destinations = metadata
        .destinations
        .into_iter()
        .map(|destination| {
            CString::new(destination.replace('\0', "\\0"))
                .unwrap_or_else(|_| CString::new("").unwrap())
                .into_raw()
        })
        .collect::<Vec<_>>();
    let destinations_len = destinations.len();
    let mut destinations = destinations.into_boxed_slice();
    let result = BifroRuleMetadata {
        rule_index: metadata.rule_index as u16,
        destinations: destinations.as_mut_ptr(),
        destinations_len,
    };
    std::mem::forget(destinations);
    result
}

fn free_rule_metadata_inner(metadata_ref: &mut BifroRuleMetadata) {
    unsafe {
        if !metadata_ref.destinations.is_null() && metadata_ref.destinations_len > 0 {
            let destinations = Vec::from_raw_parts(
                metadata_ref.destinations,
                metadata_ref.destinations_len,
                metadata_ref.destinations_len,
            );
            for destination in destinations {
                if !destination.is_null() {
                    let _ = CString::from_raw(destination);
                }
            }
            metadata_ref.destinations = ptr::null_mut();
            metadata_ref.destinations_len = 0;
        }
    }
}

#[no_mangle]
pub extern "C" fn bre_create_with_config(config_path: *const c_char) -> *mut BifroRE {
    bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode_and_protobuf_schema(
        config_path,
        BifroREPayloadFormat::Json as c_int,
        ptr::null(),
        BifroRENotifyMode::Poll as c_int,
        ptr::null(),
        ptr::null(),
    )
}

#[no_mangle]
pub extern "C" fn bre_create_with_config_and_payload_format(
    config_path: *const c_char,
    payload_format: c_int,
) -> *mut BifroRE {
    bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode_and_protobuf_schema(
        config_path,
        payload_format,
        ptr::null(),
        BifroRENotifyMode::Poll as c_int,
        ptr::null(),
        ptr::null(),
    )
}

#[no_mangle]
pub extern "C" fn bre_create_with_config_and_payload_format_and_client_ids_path(
    config_path: *const c_char,
    payload_format: c_int,
    client_ids_path: *const c_char,
) -> *mut BifroRE {
    bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode_and_protobuf_schema(
        config_path,
        payload_format,
        client_ids_path,
        BifroRENotifyMode::Poll as c_int,
        ptr::null(),
        ptr::null(),
    )
}

#[no_mangle]
pub extern "C" fn bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode(
    config_path: *const c_char,
    payload_format: c_int,
    client_ids_path: *const c_char,
    notify_mode: c_int,
) -> *mut BifroRE {
    bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode_and_protobuf_schema(
        config_path,
        payload_format,
        client_ids_path,
        notify_mode,
        ptr::null(),
        ptr::null(),
    )
}

#[no_mangle]
pub extern "C" fn bre_create_with_config_and_payload_format_and_client_ids_path_and_notify_mode_and_protobuf_schema(
    config_path: *const c_char,
    payload_format: c_int,
    client_ids_path: *const c_char,
    notify_mode: c_int,
    protobuf_descriptor_set_path: *const c_char,
    protobuf_message_name: *const c_char,
) -> *mut BifroRE {
    ensure_logger_initialized();
    if config_path.is_null() {
        return ptr::null_mut();
    }
    let Some(payload_format) = PayloadFormat::from_ffi_code(payload_format) else {
        return ptr::null_mut();
    };
    let notify_mode = match notify_mode {
        0 => BifroRENotifyMode::Poll,
        1 => BifroRENotifyMode::PushNotify,
        _ => return ptr::null_mut(),
    };

    let config_path = unsafe { CStr::from_ptr(config_path) };
    let Ok(config_path) = config_path.to_str() else {
        return ptr::null_mut();
    };
    let client_ids_path = if client_ids_path.is_null() {
        DEFAULT_CLIENT_IDS_PATH.to_string()
    } else {
        let parsed = unsafe { CStr::from_ptr(client_ids_path) }
            .to_str()
            .ok()
            .map(|value| value.trim().to_string())
            .unwrap_or_default();
        if parsed.is_empty() {
            DEFAULT_CLIENT_IDS_PATH.to_string()
        } else {
            parsed
        }
    };

    let mut rule_engine = if payload_format == PayloadFormat::Protobuf {
        if protobuf_descriptor_set_path.is_null() || protobuf_message_name.is_null() {
            log::warn!("protobuf payload format requires descriptor_set_path and message_name");
            return ptr::null_mut();
        }
        let descriptor_path = match unsafe { CStr::from_ptr(protobuf_descriptor_set_path) }.to_str() {
            Ok(value) if !value.trim().is_empty() => value.trim(),
            _ => return ptr::null_mut(),
        };
        let message_name = match unsafe { CStr::from_ptr(protobuf_message_name) }.to_str() {
            Ok(value) if !value.trim().is_empty() => value.trim(),
            _ => return ptr::null_mut(),
        };
        match RuleEngine::with_protobuf_descriptor_set_file(
            descriptor_path,
            message_name,
        ) {
            Ok(engine) => engine,
            Err(err) => {
                log::warn!(
                    "failed to initialize protobuf decoder descriptor_set={} message={} error={}",
                    descriptor_path,
                    message_name,
                    err
                );
                return ptr::null_mut();
            }
        }
    } else {
        RuleEngine::with_json()
    };
    if rule_engine.load_rules_from_json(config_path).is_err() {
        return ptr::null_mut();
    }

    let (notify_read_fd, notify_write_fd) = match notify_mode {
        BifroRENotifyMode::Poll => (-1, -1),
        BifroRENotifyMode::PushNotify => create_notify_pipe().unwrap_or((-1, -1)),
    };
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
        poll_batch_limit: DEFAULT_POLL_BATCH_SIZE,
        client_ids_path,
        active_client_ids: Vec::new(),
        ffi_metrics: FfiMetrics::default(),
    };
    log::info!(
        "BifroRE created with config path={} payload_format={:?} notify_mode={:?}",
        config_path,
        payload_format,
        notify_mode
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
        disconnect_engine(&mut boxed);
        stop_core_worker(&mut boxed, true);
        close_notify_pipe(boxed.notify_read_fd);
        close_notify_pipe(boxed.notify_write_fd);
        boxed.notify_read_fd = -1;
        boxed.notify_write_fd = -1;
        log::info!("BifroRE destroyed");
        drop(boxed);
    }
}

#[no_mangle]
pub extern "C" fn bre_disconnect(engine: *mut BifroRE) -> c_int {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &mut *engine };
    disconnect_engine(engine);
    0
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
    out_snapshot: *mut BifroREMetricsSnapshot,
) -> c_int {
    if engine.is_null() || out_snapshot.is_null() {
        return -1;
    }
    let engine_ref = unsafe { &*engine };
    let eval_snapshot = {
        let guard = engine_ref.inner.lock().unwrap();
        guard.metrics().snapshot()
    };
    let ffi_snapshot = engine_ref.ffi_metrics.snapshot();
    let snapshot = combine_metrics_snapshot(ffi_snapshot, eval_snapshot);
    unsafe {
        ptr::write(out_snapshot, snapshot);
    }
    0
}

#[no_mangle]
pub extern "C" fn bre_get_rule_metadata_table(
    engine: *const BifroRE,
    out_metadata: *mut *mut BifroRuleMetadata,
    out_len: *mut size_t,
) -> c_int {
    if engine.is_null() || out_metadata.is_null() || out_len.is_null() {
        return -1;
    }
    let guard = unsafe { &*engine }.inner.lock().unwrap();
    let metadata = guard
        .rule_metadata()
        .into_iter()
        .map(to_ffi_rule_metadata)
        .collect::<Vec<_>>();
    let mut boxed = metadata.into_boxed_slice();
    let len = boxed.len();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    unsafe {
        *out_metadata = ptr;
        *out_len = len;
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
pub extern "C" fn bre_set_poll_batch_limit(
    engine: *mut BifroRE,
    limit: u32,
) -> c_int {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &mut *engine };
    engine.poll_batch_limit = if limit == 0 {
        DEFAULT_POLL_BATCH_SIZE
    } else {
        limit as usize
    };
    0
}

#[no_mangle]
pub extern "C" fn bre_start_mqtt(
    engine: *mut BifroRE,
    host: *const c_char,
    port: u16,
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
    let group_name = unsafe { CStr::from_ptr(group_name) };
    let ordered_prefix = unsafe { CStr::from_ptr(ordered_prefix) };
    let host = match host.to_str() {
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
    let requested_client_count = client_count.max(1);
    let client_ids = resolve_client_ids(&engine_ref.client_ids_path, &node_id, requested_client_count);
    let effective_client_count = client_ids.len().max(1) as u16;
    if effective_client_count != requested_client_count {
        log::warn!(
            "client id file count overrides requested client_count: requested={} actual={}",
            requested_client_count,
            effective_client_count
        );
    }

    let config = MqttConfig {
        host,
        port,
        node_id,
        client_count: effective_client_count,
        client_ids: client_ids.clone(),
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
    engine_ref.active_client_ids = client_ids;
    if let Err(err) = persist_client_ids(&engine_ref.client_ids_path, &engine_ref.active_client_ids) {
        log::warn!(
            "failed to persist client ids at start path={} error={}",
            engine_ref.client_ids_path,
            err
        );
    }
    log::info!(
        "Starting MQTT from FFI host={} port={} node_id={} clients={}",
        config.host,
        config.port,
        config.node_id,
        config.client_count
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
            engine.ffi_metrics.record_core_queue_dequeued();
            let mut guard = engine.inner.lock().unwrap();
            let results = guard.evaluate(message);
            drop(guard);
            delivery.ack();
            let mut enqueued_any = false;
            for result in results {
                let eval_result = EvalResultRecord {
                    rule_index: result.rule_index as u16,
                    payload: result.message.payload,
                };
                match eval_result_tx_for_worker.try_send(eval_result) {
                    Ok(()) => {
                        engine.ffi_metrics.record_ffi_queue_enqueued();
                        enqueued_any = true;
                    }
                    Err(flume::TrySendError::Full(_)) => {
                        engine.ffi_metrics.record_ffi_queue_drop();
                        log::warn!("dropping eval result because poll queue is full");
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {
                        engine.ffi_metrics.record_ffi_queue_drop();
                        log::warn!("dropping eval result because poll queue is closed");
                        break;
                    }
                }
            }
            if enqueued_any {
                maybe_notify_eval_ready(notify_write_fd, &engine.notify_pending);
            }
        }
    });

    let core_queue_tx_for_handler = core_queue_tx.clone();
    let engine_ptr_for_handler = engine as usize;
    let handler: MessageHandler = std::sync::Arc::new(move |delivery: IncomingDelivery| {
        let engine = unsafe { &*(engine_ptr_for_handler as *const BifroRE) };
        engine.ffi_metrics.record_ingress();
        if core_queue_tx_for_handler.try_send(delivery).is_err() {
            engine.ffi_metrics.record_core_queue_drop();
            log::warn!("dropping incoming message because core queue is full or closed");
        } else {
            engine.ffi_metrics.record_core_queue_enqueued();
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

fn disconnect_engine(engine: &mut BifroRE) {
    if let Some(adapter) = engine.adapter.take() {
        let _ = adapter.stop();
        log::info!("BifroRE disconnect requested MQTT stop");
    }
    stop_core_worker(engine, false);
}

fn stop_core_worker(engine: &mut BifroRE, drop_receiver: bool) {
    engine.core_queue_tx.take();
    engine.eval_result_tx.take();
    if let Some(worker) = engine.core_worker.take() {
        let _ = worker.join();
    }
    if drop_receiver {
        engine.eval_result_rx.take();
    }
}

#[no_mangle]
pub extern "C" fn bre_poll_eval_results_packed(
    engine: *mut BifroRE,
    timeout_millis: u32,
    out_results: *mut BifroPackedEvalResults,
) -> c_int {
    if engine.is_null() || out_results.is_null() {
        return -1;
    }
    unsafe {
        *out_results = BifroPackedEvalResults {
            rule_indices: ptr::null_mut(),
            payload_offsets: ptr::null_mut(),
            payload_lengths: ptr::null_mut(),
            payload_data: ptr::null_mut(),
            len: 0,
            payload_data_len: 0,
        };
    }

    let engine = unsafe { &mut *engine };
    let Some(receiver) = engine.eval_result_rx.as_ref() else {
        return -2;
    };
    let max_results = engine.poll_batch_limit.max(1);

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

    let mut records = Vec::with_capacity(max_results);
    let mut total_payload_len = first_record.payload.len();
    records.push(first_record);

    while records.len() < max_results {
        match receiver.try_recv() {
            Ok(record) => {
                total_payload_len += record.payload.len();
                records.push(record);
            }
            Err(flume::TryRecvError::Empty) => break,
            Err(flume::TryRecvError::Disconnected) => break,
        }
    }

    if receiver.is_empty() {
        engine.notify_pending.store(false, Ordering::Release);
    }
    engine.ffi_metrics.record_ffi_queue_dequeued(records.len() as u64);

    let mut rule_indices = Vec::with_capacity(records.len());
    let mut payload_offsets = Vec::with_capacity(records.len());
    let mut payload_lengths = Vec::with_capacity(records.len());
    let mut payload_data = Vec::with_capacity(total_payload_len);

    for record in records {
        let offset = payload_data.len() as u32;
        let payload_len = record.payload.len() as u32;
        rule_indices.push(record.rule_index as u32);
        payload_offsets.push(offset);
        payload_lengths.push(payload_len);
        payload_data.extend_from_slice(&record.payload);
    }

    let len = rule_indices.len();
    let mut rule_indices = rule_indices.into_boxed_slice();
    let mut payload_offsets = payload_offsets.into_boxed_slice();
    let mut payload_lengths = payload_lengths.into_boxed_slice();
    let payload_data_len = payload_data.len();
    let mut payload_data = payload_data.into_boxed_slice();

    unsafe {
        *out_results = BifroPackedEvalResults {
            rule_indices: rule_indices.as_mut_ptr(),
            payload_offsets: payload_offsets.as_mut_ptr(),
            payload_lengths: payload_lengths.as_mut_ptr(),
            payload_data: payload_data.as_mut_ptr(),
            len,
            payload_data_len,
        };
    }
    std::mem::forget(rule_indices);
    std::mem::forget(payload_offsets);
    std::mem::forget(payload_lengths);
    std::mem::forget(payload_data);
    1
}

#[no_mangle]
pub extern "C" fn bre_free_packed_eval_results(results: *mut BifroPackedEvalResults) {
    if results.is_null() {
        return;
    }
    let results = unsafe { &mut *results };
    free_packed_eval_results_inner(results);
}

#[no_mangle]
pub extern "C" fn bre_free_rule_metadata_table(
    metadata: *mut BifroRuleMetadata,
    len: size_t,
) {
    if metadata.is_null() || len == 0 {
        return;
    }
    unsafe {
        let mut records = Vec::from_raw_parts(metadata, len, len);
        for record in &mut records {
            free_rule_metadata_inner(record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_client_ids_uses_file_when_present() {
        let path = "/tmp/bifrore-test-client-ids";
        let _ = fs::write(path, "cid-a\ncid-b\n");
        let ids = resolve_client_ids(path, "node-1", 2);
        assert_eq!(ids, vec!["cid-a".to_string(), "cid-b".to_string()]);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn resolve_client_ids_generates_plain_defaults_when_missing() {
        let ids = resolve_client_ids("/tmp/bifrore-test-client-ids-missing", "node-1", 3);
        assert_eq!(
            ids,
            vec![
                "node-1_0".to_string(),
                "node-1_1".to_string(),
                "node-1_2".to_string()
            ]
        );
    }
}
