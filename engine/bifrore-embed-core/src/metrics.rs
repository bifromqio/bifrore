use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyStage {
    TopicMatch = 0,
    PayloadDecode = 1,
    MsgIrBuild = 2,
    Predicate = 3,
    Projection = 4,
    Exec = 5,
}

impl LatencyStage {
    const COUNT: usize = 6;

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
        self.total_nanos
            .fetch_add(duration_nanos, Ordering::Relaxed);

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

#[derive(Clone, Copy, Debug)]
pub struct StageTimer(Option<Instant>);

#[derive(Clone, Copy, Debug)]
struct StageRecorder {
    start: fn() -> StageTimer,
    finish: fn(&LatencyMetrics, StageTimer),
}

fn disabled_stage_start() -> StageTimer {
    StageTimer(None)
}

fn disabled_stage_finish(_metrics: &LatencyMetrics, _timer: StageTimer) {}

fn detailed_stage_start() -> StageTimer {
    StageTimer(Some(Instant::now()))
}

fn detailed_stage_finish(metrics: &LatencyMetrics, timer: StageTimer) {
    let start = timer
        .0
        .expect("detailed stage timer must carry a start instant");
    metrics.record(start.elapsed().as_nanos() as u64);
}

impl StageRecorder {
    const DISABLED: Self = Self {
        start: disabled_stage_start,
        finish: disabled_stage_finish,
    };

    const DETAILED: Self = Self {
        start: detailed_stage_start,
        finish: detailed_stage_finish,
    };
}

#[derive(Debug)]
pub struct EvalMetrics {
    eval_count: AtomicU64,
    eval_error_count: AtomicU64,
    eval_type_error_count: AtomicU64,
    payload_error_count: AtomicU64,
    detailed_latency_enabled: bool,
    stage_recorder: StageRecorder,
    stages: [LatencyMetrics; LatencyStage::COUNT],
}

impl EvalMetrics {
    pub fn new(detailed_latency_enabled: bool) -> Self {
        Self {
            eval_count: AtomicU64::new(0),
            eval_error_count: AtomicU64::new(0),
            eval_type_error_count: AtomicU64::new(0),
            payload_error_count: AtomicU64::new(0),
            detailed_latency_enabled,
            stage_recorder: if detailed_latency_enabled {
                StageRecorder::DETAILED
            } else {
                StageRecorder::DISABLED
            },
            stages: std::array::from_fn(|_| LatencyMetrics::default()),
        }
    }

    pub fn detailed_latency_enabled(&self) -> bool {
        self.detailed_latency_enabled
    }

    #[inline(always)]
    pub fn start_stage(&self) -> StageTimer {
        (self.stage_recorder.start)()
    }

    #[inline(always)]
    pub fn finish_stage(&self, stage: LatencyStage, timer: StageTimer) {
        (self.stage_recorder.finish)(&self.stages[stage.index()], timer);
    }

    pub fn record_eval_success(&self) {
        self.eval_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eval_error(&self, is_type_error: bool) {
        self.eval_count.fetch_add(1, Ordering::Relaxed);
        self.eval_error_count.fetch_add(1, Ordering::Relaxed);
        if is_type_error {
            self.eval_type_error_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_payload_error(&self) {
        self.payload_error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> EvalMetricsSnapshot {
        EvalMetricsSnapshot {
            eval_count: self.eval_count.load(Ordering::Relaxed),
            eval_error_count: self.eval_error_count.load(Ordering::Relaxed),
            eval_type_error_count: self.eval_type_error_count.load(Ordering::Relaxed),
            payload_error_count: self.payload_error_count.load(Ordering::Relaxed),
            topic_match: self.stages[LatencyStage::TopicMatch.index()].snapshot(),
            payload_decode: self.stages[LatencyStage::PayloadDecode.index()].snapshot(),
            msg_ir_build: self.stages[LatencyStage::MsgIrBuild.index()].snapshot(),
            predicate: self.stages[LatencyStage::Predicate.index()].snapshot(),
            projection: self.stages[LatencyStage::Projection.index()].snapshot(),
            exec: self.stages[LatencyStage::Exec.index()].snapshot(),
        }
    }
}

impl Default for EvalMetrics {
    fn default() -> Self {
        Self::new(false)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LatencyMetricsSnapshot {
    pub count: u64,
    pub total_nanos: u64,
    pub max_nanos: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EvalMetricsSnapshot {
    pub eval_count: u64,
    pub eval_error_count: u64,
    pub eval_type_error_count: u64,
    pub payload_error_count: u64,
    pub topic_match: LatencyMetricsSnapshot,
    pub payload_decode: LatencyMetricsSnapshot,
    pub msg_ir_build: LatencyMetricsSnapshot,
    pub predicate: LatencyMetricsSnapshot,
    pub projection: LatencyMetricsSnapshot,
    pub exec: LatencyMetricsSnapshot,
}
