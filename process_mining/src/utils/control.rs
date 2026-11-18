//! Progress reporting, cancellation and partial results for long-running operations.
//!
//! A [`Control`] is handed to an operation, which reports [`Progress`] and warnings through it,
//! checks whether it was cancelled, and streams partial results. The long-running operations of
//! this crate are methods on [`Control`] (e.g., `Control::align_variants`), next to their plain
//! counterparts, which run without one. Imports take one through
//! [`Importable`](crate::Importable)'s `_with` methods (e.g., `import_from_path_with`).
//!
//! ```
//! use std::sync::atomic::AtomicBool;
//! use process_mining::utils::control::Control;
//!
//! let cancel = AtomicBool::new(false);
//! let ctl = Control::new()
//!     .on_progress(|p| eprintln!("{}: {}/{:?}", p.stage, p.done, p.total))
//!     .cancel_flag(&cancel);
//! assert!(ctl.check().is_ok());
//! ```
//!
//! The caller owns everything else: threads, job ids, how progress reaches a user, and when to
//! set the cancel flag.

use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How far an operation got.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Progress {
    /// What the operation is doing, e.g., `"Aligning variants"`.
    pub stage: Cow<'static, str>,
    /// Units of work finished in this stage.
    pub done: u64,
    /// Units of work in this stage, when known in advance.
    pub total: Option<u64>,
}

/// The error an operation returns once its [`Control`] was cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// Message for unwrapping the result of an operation run with [`Control::new`].
pub(crate) const NEVER_CANCELLED: &str = "a new Control is never cancelled";

type ProgressFn<'a> = dyn Fn(Progress) + Send + Sync + 'a;
type PartialFn<'a> = dyn Fn(serde_json::Value) + Send + Sync + 'a;
type WarningFn<'a> = dyn Fn(&str) + Send + Sync + 'a;

/// Receives progress and partial results from an operation and tells it when to stop.
///
/// Every part is optional: [`Control::new`] reports nowhere and is never cancelled. It is `Sync`,
/// so parallel operations report from several threads at once.
#[derive(Default)]
pub struct Control<'a> {
    progress: Option<Box<ProgressFn<'a>>>,
    partial: Option<Box<PartialFn<'a>>>,
    warning: Option<Box<WarningFn<'a>>>,
    cancelled: Option<&'a AtomicBool>,
}

impl fmt::Debug for Control<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Control")
            .field("progress", &self.progress.is_some())
            .field("partial", &self.partial.is_some())
            .field("warning", &self.warning.is_some())
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl<'a> Control<'a> {
    /// A control that reports nowhere and is never cancelled.
    pub const fn new() -> Self {
        Self {
            progress: None,
            partial: None,
            warning: None,
            cancelled: None,
        }
    }

    /// Call `f` with every progress report.
    pub fn on_progress(mut self, f: impl Fn(Progress) + Send + Sync + 'a) -> Self {
        self.progress = Some(Box::new(f));
        self
    }

    /// Call `f` with every partial result. What one partial value is depends on the operation and
    /// is documented there.
    pub fn on_partial(mut self, f: impl Fn(serde_json::Value) + Send + Sync + 'a) -> Self {
        self.partial = Some(Box::new(f));
        self
    }

    /// Call `f` with every warning, instead of logging it with `log::warn!`.
    pub fn on_warning(mut self, f: impl Fn(&str) + Send + Sync + 'a) -> Self {
        self.warning = Some(Box::new(f));
        self
    }

    /// Stop the operation once `flag` is set.
    pub fn cancel_flag(mut self, flag: &'a AtomicBool) -> Self {
        self.cancelled = Some(flag);
        self
    }

    /// Whether the cancel flag is set.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.is_some_and(|c| c.load(Ordering::Relaxed))
    }

    /// `Err(Cancelled)` once the cancel flag is set.
    pub fn check(&self) -> Result<(), Cancelled> {
        if self.is_cancelled() {
            Err(Cancelled)
        } else {
            Ok(())
        }
    }

    /// The cancel flag, for code that only needs to poll it (e.g., a search loop).
    pub fn cancel_flag_ref(&self) -> Option<&'a AtomicBool> {
        self.cancelled
    }

    /// Report progress.
    pub fn progress(&self, stage: impl Into<Cow<'static, str>>, done: u64, total: Option<u64>) {
        if let Some(f) = &self.progress {
            f(Progress {
                stage: stage.into(),
                done,
                total,
            });
        }
    }

    /// Whether anyone listens for progress, so an operation can skip work done only to report
    /// it (e.g., counting rows in advance).
    pub fn wants_progress(&self) -> bool {
        self.progress.is_some()
    }

    /// Report a warning to the [`Control::on_warning`] listener, or log it with `log::warn!`
    /// when there is none.
    pub fn warn(&self, msg: impl fmt::Display) {
        match &self.warning {
            Some(f) => f(&msg.to_string()),
            None => log::warn!("{msg}"),
        }
    }

    /// Whether anyone listens for partial results, so an operation can skip building them.
    pub fn wants_partials(&self) -> bool {
        self.partial.is_some()
    }

    /// Emit one partial result. It is serialized only when someone listens.
    pub fn partial<T: Serialize + ?Sized>(&self, value: &T) {
        if let Some(f) = &self.partial {
            if let Ok(value) = serde_json::to_value(value) {
                f(value);
            }
        }
    }

    /// A counter for a stage of `total` units, safe to tick from parallel workers. It reports
    /// the start right away.
    pub fn counter(
        &self,
        stage: impl Into<Cow<'static, str>>,
        total: Option<u64>,
    ) -> Counter<'_, 'a> {
        let stage = stage.into();
        self.progress(stage.clone(), 0, total);
        Counter {
            ctl: self,
            stage,
            total,
            done: AtomicU64::new(0),
            reported: Mutex::new(0),
        }
    }
}

/// Counts finished units of one stage and reports each step. See [`Control::counter`].
#[derive(Debug)]
pub struct Counter<'c, 'a> {
    ctl: &'c Control<'a>,
    stage: Cow<'static, str>,
    total: Option<u64>,
    done: AtomicU64,
    /// The last `done` reported. Held while reporting, so reports from parallel workers reach
    /// the callback in increasing order.
    reported: Mutex<u64>,
}

impl Counter<'_, '_> {
    /// Count `n` more finished units and report, unless a parallel worker already reported a
    /// higher count.
    pub fn add(&self, n: u64) {
        let done = self.done.fetch_add(n, Ordering::Relaxed) + n;
        if !self.ctl.wants_progress() {
            return;
        }
        let mut reported = self.reported.lock().unwrap_or_else(PoisonError::into_inner);
        if done > *reported {
            *reported = done;
            self.ctl.progress(self.stage.clone(), done, self.total);
        }
    }

    /// Count one more finished unit and report.
    pub fn tick(&self) {
        self.add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_progress_partials_and_cancel() {
        let flag = AtomicBool::new(false);
        let seen = Mutex::new(Vec::new());
        let partials = Mutex::new(Vec::new());
        let ctl = Control::new()
            .on_progress(|p| {
                seen.lock()
                    .unwrap()
                    .push((p.stage.into_owned(), p.done, p.total))
            })
            .on_partial(|v| partials.lock().unwrap().push(v))
            .cancel_flag(&flag);
        let counter = ctl.counter("work", Some(2));
        counter.tick();
        counter.tick();
        ctl.partial(&serde_json::json!({ "i": 1 }));
        assert!(ctl.check().is_ok());
        flag.store(true, Ordering::Relaxed);
        assert_eq!(ctl.check(), Err(Cancelled));
        drop(ctl);
        assert_eq!(
            seen.into_inner().unwrap(),
            vec![
                ("work".to_string(), 0, Some(2)),
                ("work".to_string(), 1, Some(2)),
                ("work".to_string(), 2, Some(2))
            ]
        );
        assert_eq!(
            partials.into_inner().unwrap(),
            vec![serde_json::json!({ "i": 1 })]
        );
    }

    #[test]
    fn parallel_counter_reports_increasing_counts() {
        let seen = Mutex::new(Vec::new());
        let ctl = Control::new().on_progress(|p| seen.lock().unwrap().push(p.done));
        let counter = ctl.counter("work", Some(4000));
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| (0..1000).for_each(|_| counter.tick()));
            }
        });
        drop(ctl);
        let seen = seen.into_inner().unwrap();
        assert!(seen.windows(2).all(|w| w[0] < w[1]), "{seen:?}");
        assert_eq!(seen.last(), Some(&4000));
    }

    #[test]
    fn warnings_reach_the_listener() {
        let seen = Mutex::new(Vec::new());
        let ctl = Control::new().on_warning(|w| seen.lock().unwrap().push(w.to_string()));
        ctl.warn(format_args!("row {}", 3));
        drop(ctl);
        assert_eq!(seen.into_inner().unwrap(), vec!["row 3".to_string()]);
    }

    #[test]
    fn default_control_is_inert() {
        let ctl = Control::new();
        ctl.progress("x", 1, None);
        ctl.partial(&1);
        ctl.warn("x");
        assert!(!ctl.wants_partials());
        assert!(!ctl.wants_progress());
        assert!(ctl.check().is_ok());
    }
}
