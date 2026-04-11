use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyStage {
    TopicMatch = 0,
    PayloadDecode = 1,
    MsgIrBuild = 2,
    FastWhere = 3,
    Predicate = 4,
    Projection = 5,
    EvalTotal = 6,
}

impl LatencyStage {
    const COUNT: usize = 7;

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug)]
struct LatencyMetrics {
    count: AtomicU64,
    total_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl Default for LatencyMetrics {
    fn default() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_nanos: AtomicU64::new(0),
            max_nanos: AtomicU64::new(0),
        }
    }
}

impl LatencyMetrics {
    fn record(&self, duration_nanos: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_nanos.fetch_add(duration_nanos, Ordering::Relaxed);

        let mut current = self.max_nanos.load(Ordering::Relaxed);
        while duration_nanos > current {
            match self.max_nanos.compare_exchange_weak(
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

    fn snapshot(&self) -> LatencyMetricsSnapshot {
        LatencyMetricsSnapshot {
            count: self.count.load(Ordering::Relaxed),
            total_nanos: self.total_nanos.load(Ordering::Relaxed),
            max_nanos: self.max_nanos.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
pub struct EvalMetrics {
    eval_count: AtomicU64,
    eval_error_count: AtomicU64,
    stages: [LatencyMetrics; LatencyStage::COUNT],
}

impl EvalMetrics {
    pub fn record_stage(&self, stage: LatencyStage, duration_nanos: u64) {
        self.stages[stage.index()].record(duration_nanos);
    }

    pub fn record_eval(&self, duration_nanos: u64, success: bool) {
        self.eval_count.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.eval_error_count.fetch_add(1, Ordering::Relaxed);
        }
        self.record_stage(LatencyStage::EvalTotal, duration_nanos);
    }

    pub fn snapshot(&self) -> EvalMetricsSnapshot {
        EvalMetricsSnapshot {
            eval_count: self.eval_count.load(Ordering::Relaxed),
            eval_error_count: self.eval_error_count.load(Ordering::Relaxed),
            topic_match: self.stages[LatencyStage::TopicMatch.index()].snapshot(),
            payload_decode: self.stages[LatencyStage::PayloadDecode.index()].snapshot(),
            msg_ir_build: self.stages[LatencyStage::MsgIrBuild.index()].snapshot(),
            fast_where: self.stages[LatencyStage::FastWhere.index()].snapshot(),
            predicate: self.stages[LatencyStage::Predicate.index()].snapshot(),
            projection: self.stages[LatencyStage::Projection.index()].snapshot(),
            eval_total: self.stages[LatencyStage::EvalTotal.index()].snapshot(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LatencyMetricsSnapshot {
    pub count: u64,
    pub total_nanos: u64,
    pub max_nanos: u64,
}

impl LatencyMetricsSnapshot {
    pub fn avg_nanos(&self) -> Option<u64> {
        if self.count == 0 {
            None
        } else {
            Some(self.total_nanos / self.count)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EvalMetricsSnapshot {
    pub eval_count: u64,
    pub eval_error_count: u64,
    pub topic_match: LatencyMetricsSnapshot,
    pub payload_decode: LatencyMetricsSnapshot,
    pub msg_ir_build: LatencyMetricsSnapshot,
    pub fast_where: LatencyMetricsSnapshot,
    pub predicate: LatencyMetricsSnapshot,
    pub projection: LatencyMetricsSnapshot,
    pub eval_total: LatencyMetricsSnapshot,
}

impl EvalMetricsSnapshot {
    pub fn avg_nanos(&self) -> Option<u64> {
        self.eval_total.avg_nanos()
    }
}
