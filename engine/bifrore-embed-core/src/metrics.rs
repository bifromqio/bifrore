use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct EvalMetrics {
    eval_count: AtomicU64,
    eval_error_count: AtomicU64,
    eval_total_nanos: AtomicU64,
    eval_max_nanos: AtomicU64,
}

impl EvalMetrics {
    pub fn record(&self, duration_nanos: u64, success: bool) {
        self.eval_count.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.eval_error_count.fetch_add(1, Ordering::Relaxed);
        }
        self.eval_total_nanos
            .fetch_add(duration_nanos, Ordering::Relaxed);

        let mut current = self.eval_max_nanos.load(Ordering::Relaxed);
        while duration_nanos > current {
            match self.eval_max_nanos.compare_exchange_weak(
                current,
                duration_nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }

    pub fn snapshot(&self) -> EvalMetricsSnapshot {
        EvalMetricsSnapshot {
            eval_count: self.eval_count.load(Ordering::Relaxed),
            eval_error_count: self.eval_error_count.load(Ordering::Relaxed),
            eval_total_nanos: self.eval_total_nanos.load(Ordering::Relaxed),
            eval_max_nanos: self.eval_max_nanos.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvalMetricsSnapshot {
    pub eval_count: u64,
    pub eval_error_count: u64,
    pub eval_total_nanos: u64,
    pub eval_max_nanos: u64,
}

impl EvalMetricsSnapshot {
    pub fn avg_nanos(&self) -> Option<u64> {
        if self.eval_count == 0 {
            None
        } else {
            Some(self.eval_total_nanos / self.eval_count)
        }
    }
}
