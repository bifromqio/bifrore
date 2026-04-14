use libc::c_int;

pub const BRE_OK: c_int = 0;
pub const BRE_POLL_RESULT_READY: c_int = 1;
pub const BRE_ERR_INVALID_ARGUMENT: c_int = -1;
pub const BRE_ERR_INVALID_STATE: c_int = -2;
pub const BRE_ERR_OPERATION_FAILED: c_int = -3;
pub const BRE_ERR_START_FAILED: c_int = -4;
pub const BRE_ERR_ALREADY_STARTED: c_int = -5;
pub const BRE_ERR_WORKER_UNAVAILABLE: c_int = -6;
