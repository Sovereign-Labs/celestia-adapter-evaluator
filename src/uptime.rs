//! Submission-path observations, independent of the block-reading workloads.
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use celestia_client::{Error as ClientError, types::state::ErrorCode};
use celestia_grpc::Error as GrpcError;
use sov_metrics::Metric;

use crate::metrics;

const MAX_ORDERED_RESULTS: usize = 64;
const STALE_AFTER: Duration = Duration::from_secs(120);

#[derive(Clone, Debug)]
struct Pending {
    start: Instant,
    result: Option<(Outcome, f64, Instant)>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Outcome {
    Success,
    Failure,
    Unknown,
}

impl Outcome {
    fn index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Unknown => "unknown",
        }
    }
}

/// Only structured evidence establishes a service failure. Ambiguous auth,
/// caller errors, local faults and unrecognized errors remain unknown.
pub fn classify(error: &anyhow::Error) -> Outcome {
    for cause in error.chain() {
        let Some(ClientError::Grpc(error)) = cause.downcast_ref() else {
            continue;
        };
        return if is_confirmed_service_failure(error) {
            Outcome::Failure
        } else {
            Outcome::Unknown
        };
    }
    Outcome::Unknown
}

fn is_confirmed_service_failure(error: &GrpcError) -> bool {
    match error {
        GrpcError::TxExecutionFailed(_, code, _)
        | GrpcError::TxBroadcastFailed(_, code, _)
        | GrpcError::TxRejected(_, code, _) => matches!(
            code,
            ErrorCode::MempoolIsFull
                | ErrorCode::Logic
                | ErrorCode::IO
                | ErrorCode::AppConfig
                | ErrorCode::Panic
        ),
        GrpcError::FailedToParseResponse | GrpcError::UnexpectedResponseType(_) => true,
        _ => error.is_network_error(),
    }
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[derive(Clone, Debug)]
struct State {
    started: u64,
    outcomes: [u64; 3],
    in_flight: u64,
    process_start: f64,
    last_progress: f64,
    last_start: f64,
    last_outcome: f64,
    duration_sum: f64,
    last_duration: f64,
    pending: BTreeMap<u64, Pending>,
    ordering_gaps: u64,
    consecutive_failures: u64,
    failure_since: Option<Instant>,
    failure_span: f64,
    last_ordered_outcome: f64,
    progress_at: Instant,
    outcome_at: Instant,
    stale: bool,
}

impl State {
    fn new(now: f64, clock: Instant) -> Self {
        Self {
            started: 0,
            outcomes: [0; 3],
            in_flight: 0,
            process_start: now,
            last_progress: 0.0,
            last_start: 0.0,
            last_outcome: 0.0,
            duration_sum: 0.0,
            last_duration: 0.0,
            pending: BTreeMap::new(),
            ordering_gaps: 0,
            consecutive_failures: 0,
            failure_since: None,
            failure_span: 0.0,
            last_ordered_outcome: 0.0,
            progress_at: clock,
            outcome_at: clock,
            stale: false,
        }
    }

    fn start(&mut self, now: f64, clock: Instant) -> u64 {
        self.expire(clock);
        if self.pending.len() == MAX_ORDERED_RESULTS {
            // Never grow an unbounded reorder queue behind a stuck submission.
            // The dropped slot is an ordering gap, not a fabricated terminal result.
            self.pending.pop_first();
            self.ordering_gaps += 1;
            self.reset_streak();
            self.drain();
        }
        self.started += 1;
        self.in_flight += 1;
        self.last_start = now;
        self.pending.insert(
            self.started,
            Pending {
                start: clock,
                result: None,
            },
        );
        self.started
    }

    fn finish(&mut self, id: u64, outcome: Outcome, now: f64, clock: Instant, duration: Duration) {
        self.expire(clock);
        self.outcomes[outcome.index()] += 1;
        self.in_flight -= 1;
        self.last_outcome = now;
        self.outcome_at = clock;
        self.last_duration = duration.as_secs_f64();
        self.duration_sum += self.last_duration;
        if outcome != Outcome::Failure {
            self.reset_streak();
        }
        // A result whose ordering evidence was lost still counts once, but
        // cannot revive a stale streak or stand in for a newer submission.
        if let Some(pending) = self.pending.get_mut(&id) {
            pending.result = Some((outcome, now, clock));
        }
        self.drain();
    }

    fn reset_streak(&mut self) {
        self.consecutive_failures = 0;
        self.failure_since = None;
        self.failure_span = 0.0;
    }

    fn expire(&mut self, clock: Instant) {
        let stale = clock.saturating_duration_since(self.progress_at) >= STALE_AFTER
            || clock.saturating_duration_since(self.outcome_at) >= STALE_AFTER;
        if stale {
            if !self.stale {
                self.ordering_gaps += 1;
            }
            self.reset_streak();
            self.pending.clear();
        }
        self.stale = stale;
    }

    fn drain(&mut self) {
        while self
            .pending
            .first_key_value()
            .is_some_and(|(_, value)| value.result.is_some())
        {
            let (_, pending) = self.pending.pop_first().unwrap();
            let (outcome, wall, clock) = pending.result.unwrap();
            self.last_ordered_outcome = wall;
            if outcome == Outcome::Failure {
                self.consecutive_failures += 1;
                let since = self.failure_since.get_or_insert(pending.start);
                self.failure_span = clock.saturating_duration_since(*since).as_secs_f64();
            } else {
                self.reset_streak();
            }
        }
        if self.pending.values().any(|pending| {
            pending
                .result
                .is_some_and(|(outcome, _, _)| outcome != Outcome::Failure)
        }) {
            self.reset_streak();
        }
    }
}

#[derive(Clone)]
pub struct Tracker(Arc<Mutex<State>>);

impl Tracker {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(State::new(unix_now(), Instant::now()))))
    }

    /// Called by the actual submission loop, never by the metrics publisher.
    pub fn progress(&self) {
        let mut state = self.0.lock().unwrap();
        let clock = Instant::now();
        state.expire(clock);
        state.last_progress = unix_now();
        state.progress_at = clock;
    }

    pub fn start(&self) -> Observation {
        let start = Instant::now();
        let id = self.0.lock().unwrap().start(unix_now(), start);
        Observation {
            tracker: self.clone(),
            start,
            id,
            finished: false,
        }
    }

    pub fn emit(&self) {
        let snapshot = {
            let mut state = self.0.lock().unwrap();
            state.expire(Instant::now());
            state.clone()
        };
        metrics::emit(snapshot.clone());
        metrics::emit(StreakMeasurement {
            failures: snapshot.consecutive_failures,
            span: snapshot.failure_span,
            last_ordered_outcome: snapshot.last_ordered_outcome,
        });
        for outcome in [Outcome::Success, Outcome::Failure, Outcome::Unknown] {
            metrics::emit(OutcomeMeasurement {
                outcome,
                count: snapshot.outcomes[outcome.index()],
            });
        }
    }

    pub fn spawn_publisher(
        &self,
        shutdown: sov_rollup_interface::node::SecondaryShutdownController,
    ) {
        let tracker = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => tracker.emit(),
                    _ = shutdown.wait_for_shutdown() => {
                        tracker.emit();
                        break;
                    }
                }
            }
        });
    }
}

/// Dropping an unfinished observation accounts for cancellation/task abort once.
pub struct Observation {
    tracker: Tracker,
    start: Instant,
    id: u64,
    finished: bool,
}

impl Observation {
    pub fn finish(mut self, outcome: Outcome) {
        self.tracker.0.lock().unwrap().finish(
            self.id,
            outcome,
            unix_now(),
            Instant::now(),
            self.start.elapsed(),
        );
        self.finished = true;
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        if !self.finished {
            self.tracker.0.lock().unwrap().finish(
                self.id,
                Outcome::Unknown,
                unix_now(),
                Instant::now(),
                self.start.elapsed(),
            );
        }
    }
}

#[derive(Debug)]
struct OutcomeMeasurement {
    outcome: Outcome,
    count: u64,
}

#[derive(Debug)]
struct StreakMeasurement {
    failures: u64,
    span: f64,
    last_ordered_outcome: f64,
}

impl Metric for StreakMeasurement {
    fn measurement_name(&self) -> &'static str {
        "evaluator_submission"
    }
    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        write!(
            buffer,
            "evaluator_submission consecutive_failures={}i,failure_streak_span_seconds={},last_ordered_outcome_time_seconds={}",
            self.failures, self.span, self.last_ordered_outcome
        )
    }
}

impl Metric for OutcomeMeasurement {
    fn measurement_name(&self) -> &'static str {
        "evaluator_submission"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        write!(
            buffer,
            "evaluator_submission,outcome={} outcomes_total={}i",
            self.outcome.name(),
            self.count
        )
    }
}

impl Metric for State {
    fn measurement_name(&self) -> &'static str {
        "evaluator_submission"
    }

    fn serialize_for_telegraf(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        write!(
            buffer,
            "evaluator_submission started_total={}i,in_flight={}i,process_start_time_seconds={},last_progress_time_seconds={},last_start_time_seconds={},last_outcome_time_seconds={},duration_seconds_sum={},last_duration_seconds={},evidence_gaps_total={}i,ordering_pending={}i",
            self.started,
            self.in_flight,
            self.process_start,
            self.last_progress,
            self.last_start,
            self.last_outcome,
            self.duration_sum,
            self.last_duration,
            self.ordering_gaps,
            self.pending.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_terminal_attempts_and_keeps_progress_distinct_from_publication() {
        let clock = Instant::now();
        let mut state = State::new(100.0, clock);
        let first = state.start(101.0, clock);
        state.start(102.0, clock);
        state.finish(
            first,
            Outcome::Failure,
            103.0,
            clock,
            Duration::from_secs(2),
        );
        assert_eq!(state.started, 2);
        assert_eq!(state.outcomes, [0, 1, 0]);
        assert_eq!(state.in_flight, 1);
        assert_eq!(state.last_progress, 0.0);
        let mut bytes = Vec::new();
        state.serialize_for_telegraf(&mut bytes).unwrap();
        assert_eq!(state.last_progress, 0.0);
        assert_eq!(state.last_outcome, 103.0);
        assert!(
            String::from_utf8(bytes)
                .unwrap()
                .contains("duration_seconds_sum=2")
        );
        assert_eq!(State::new(104.0, clock).outcomes, [0; 3]);
    }

    #[test]
    fn completion_and_cancellation_each_count_once() {
        let tracker = Tracker::new();
        tracker.start().finish(Outcome::Success);
        drop(tracker.start());
        let state = tracker.0.lock().unwrap();
        assert_eq!(state.started, 2);
        assert_eq!(state.outcomes, [1, 0, 1]);
        assert_eq!(state.in_flight, 0);
    }

    #[test]
    fn only_structured_service_evidence_is_failure() {
        use celestia_client::types::hash::Hash;
        use celestia_grpc::Error as E;
        let error = |code| {
            anyhow::Error::new(celestia_client::Error::Grpc(E::TxBroadcastFailed(
                Hash::Sha256([0; 32]),
                code,
                "ignored".into(),
            )))
            .context("Batch submit")
        };
        assert_eq!(
            classify(&error(ErrorCode::InsufficientFunds)),
            Outcome::Unknown
        );
        assert_eq!(classify(&error(ErrorCode::Unauthorized)), Outcome::Unknown);
        assert_eq!(
            classify(&error(ErrorCode::InvalidSequence)),
            Outcome::Unknown
        );
        assert_eq!(classify(&error(ErrorCode::MempoolIsFull)), Outcome::Failure);
        assert_eq!(
            classify(&anyhow::anyhow!("HTTP 500: insufficient funds")),
            Outcome::Unknown
        );
        assert_eq!(
            classify(&anyhow::anyhow!("await timeout")),
            Outcome::Unknown
        );
    }

    #[test]
    fn outcome_series_are_cumulative_with_bounded_labels() {
        let mut bytes = Vec::new();
        OutcomeMeasurement {
            outcome: Outcome::Unknown,
            count: 4,
        }
        .serialize_for_telegraf(&mut bytes)
        .unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            "evaluator_submission,outcome=unknown outcomes_total=4i"
        );
    }

    #[test]
    fn start_order_prevents_out_of_order_failures_from_overriding_success() {
        let clock = Instant::now();
        let mut state = State::new(1.0, clock);
        let a = state.start(1.0, clock);
        let b = state.start(2.0, clock + Duration::from_secs(1));
        let c = state.start(3.0, clock + Duration::from_secs(2));
        state.finish(
            c,
            Outcome::Success,
            4.0,
            clock + Duration::from_secs(3),
            Duration::from_secs(1),
        );
        state.finish(
            a,
            Outcome::Failure,
            5.0,
            clock + Duration::from_secs(4),
            Duration::from_secs(4),
        );
        assert_eq!(state.consecutive_failures, 0);
        state.finish(
            b,
            Outcome::Failure,
            6.0,
            clock + Duration::from_secs(5),
            Duration::from_secs(4),
        );
        assert_eq!(state.consecutive_failures, 0);
        let d = state.start(7.0, clock + Duration::from_secs(6));
        let e = state.start(8.0, clock + Duration::from_secs(7));
        state.finish(
            e,
            Outcome::Failure,
            9.0,
            clock + Duration::from_secs(8),
            Duration::from_secs(1),
        );
        assert_eq!(state.consecutive_failures, 0);
        state.finish(
            d,
            Outcome::Failure,
            10.0,
            clock + Duration::from_secs(9),
            Duration::from_secs(3),
        );
        assert_eq!(state.consecutive_failures, 2);
        let f = state.start(11.0, clock + Duration::from_secs(10));
        state.finish(
            f,
            Outcome::Unknown,
            12.0,
            clock + Duration::from_secs(11),
            Duration::from_secs(1),
        );
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.outcomes, [1, 4, 1]);
    }

    #[test]
    fn stale_scheduler_resets_streak_and_late_results_only_update_counters() {
        let clock = Instant::now();
        let mut state = State::new(1.0, clock);
        let a = state.start(1.0, clock);
        state.finish(
            a,
            Outcome::Failure,
            2.0,
            clock + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let b = state.start(3.0, clock + Duration::from_secs(2));
        state.expire(clock + STALE_AFTER);
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.ordering_gaps, 1);
        state.expire(clock + STALE_AFTER + Duration::from_secs(1));
        assert_eq!(state.ordering_gaps, 1);
        state.finish(
            b,
            Outcome::Failure,
            123.0,
            clock + STALE_AFTER + Duration::from_secs(2),
            STALE_AFTER,
        );
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.outcomes, [0, 2, 0]);
        assert_eq!(state.last_progress, 0.0);
        assert_eq!(State::new(124.0, clock).consecutive_failures, 0);
    }

    #[test]
    fn ordering_buffer_is_bounded_without_dropping_terminal_counts() {
        let clock = Instant::now();
        let mut state = State::new(1.0, clock);
        for _ in 0..=MAX_ORDERED_RESULTS {
            state.start(1.0, clock);
        }
        assert_eq!(state.pending.len(), MAX_ORDERED_RESULTS);
        assert_eq!(state.ordering_gaps, 1);
        state.finish(1, Outcome::Failure, 2.0, clock, Duration::ZERO);
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.outcomes, [0, 1, 0]);
        for id in 2..=MAX_ORDERED_RESULTS as u64 + 1 {
            state.finish(id, Outcome::Success, 2.0, clock, Duration::ZERO);
        }
        assert_eq!(state.in_flight, 0);
        assert_eq!(state.outcomes, [MAX_ORDERED_RESULTS as u64, 1, 0]);
    }

    #[test]
    fn concurrent_tasks_preserve_exactly_once_counts() {
        let tracker = Tracker::new();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let tracker = tracker.clone();
                scope.spawn(move || {
                    for _ in 0..100 {
                        tracker.start().finish(Outcome::Success);
                    }
                });
            }
        });
        let state = tracker.0.lock().unwrap();
        assert_eq!(state.started, 800);
        assert_eq!(state.outcomes, [800, 0, 0]);
        assert_eq!(state.in_flight, 0);
    }
}
