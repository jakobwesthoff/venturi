//! What a single execution of a handler concludes: an [`Outcome`] on success or
//! a [`TaskError`] on failure.
//!
//! The return type of [`crate::task::Handler::handle`] is
//! `Result<Outcome, TaskError>`, which encodes the four things a run can decide:
//! complete, pause, retry, or give up. Success and pause are the `Ok` variants;
//! a retryable or permanent failure is the `Err` side. Retryable is the default,
//! so any error propagated with `?` becomes a retry.

use std::time::Duration;

/// The successful conclusion of a run.
///
/// A handler returns `Ok(Outcome)` to either finish the job or pause it. A pause
/// is **not** a failure: the job returns to pending, becomes eligible again after
/// `resume_in`, keeps its carried state, and does not consume the failure
/// backstop. Use the constructors rather than the variants directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The job is finished and moves to a terminal completed state.
    Completed {
        /// Optional human-readable conclusion recorded in the journal.
        note: Option<String>,
    },
    /// The job yields and becomes eligible again after `resume_in`.
    Pause {
        /// How long until the job is eligible again; `Duration::ZERO` yields
        /// immediately.
        resume_in: Duration,
        /// Optional human-readable conclusion recorded in the journal.
        note: Option<String>,
    },
}

impl Outcome {
    /// Complete the job with no note.
    pub fn completed() -> Outcome {
        Outcome::Completed { note: None }
    }

    /// Complete the job with a note recorded in the journal.
    pub fn completed_with(note: impl Into<String>) -> Outcome {
        Outcome::Completed {
            note: Some(note.into()),
        }
    }

    /// Pause the job, making it eligible again after `resume_in`.
    pub fn pause_in(resume_in: Duration) -> Outcome {
        Outcome::Pause {
            resume_in,
            note: None,
        }
    }

    /// Pause the job with a note, making it eligible again after `resume_in`.
    pub fn pause_in_with(resume_in: Duration, note: impl Into<String>) -> Outcome {
        Outcome::Pause {
            resume_in,
            note: Some(note.into()),
        }
    }
}

/// A handler failure.
///
/// Failures are **retryable by default**: any error propagated with `?` converts
/// into a retryable `TaskError` (through the blanket [`From`] impl), and the run
/// is rescheduled with backoff. A task that knows further attempts are pointless
/// returns [`TaskError::permanent`] to send the job straight to dead. The error's
/// message becomes the journal note for the failed run.
#[derive(Debug)]
pub struct TaskError {
    permanent: bool,
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl TaskError {
    /// A retryable failure built from any error. Equivalent to the `?` conversion.
    pub fn retryable<E>(error: E) -> TaskError
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        TaskError::build(false, error)
    }

    /// A permanent failure: the job moves to dead immediately, no retry.
    pub fn permanent<E>(error: E) -> TaskError
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        TaskError::build(true, error)
    }

    /// Whether this failure sends the job straight to dead.
    pub fn is_permanent(&self) -> bool {
        self.permanent
    }

    /// The failure message recorded as the journal note.
    pub fn message(&self) -> &str {
        &self.message
    }

    fn build<E>(permanent: bool, error: E) -> TaskError
    where
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        let source = error.into();
        TaskError {
            permanent,
            message: source.to_string(),
            source: Some(source),
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl TaskError {
    /// The wrapped cause, if any.
    ///
    /// Deliberately **not** named `source`: `TaskError` does not implement
    /// [`std::error::Error`] (see the `From` impl below for why), so a `source`
    /// inherent method would shadow the trait method without participating in
    /// trait-based error-chain walking such as [`std::error::Error::source`] or
    /// `anyhow::Error::chain`.
    pub fn cause(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|boxed| boxed.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Any error propagated with `?` inside a handler becomes a retryable failure.
///
/// `TaskError` deliberately does **not** implement [`std::error::Error`]: that
/// keeps it out of this blanket conversion (which would otherwise collide with
/// the reflexive `From<T> for T`), so `?` on a foreign error yields a retryable
/// failure while a returned `permanent` error keeps its kind.
impl<E> From<E> for TaskError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(error: E) -> TaskError {
        TaskError::retryable(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("boom: {0}")]
    struct Boom(&'static str);

    #[test]
    fn question_mark_errors_are_retryable() {
        fn run() -> Result<(), TaskError> {
            Err(Boom("network"))?;
            Ok(())
        }
        let err = run().unwrap_err();
        assert!(!err.is_permanent());
        assert_eq!(err.message(), "boom: network");
    }

    #[test]
    fn permanent_is_permanent_and_keeps_message() {
        let err = TaskError::permanent(Boom("gone"));
        assert!(err.is_permanent());
        assert_eq!(err.message(), "boom: gone");
        assert_eq!(err.to_string(), "boom: gone");
    }

    #[test]
    fn cause_exposes_the_wrapped_error() {
        let err = TaskError::permanent(Boom("disk"));
        let cause = err.cause().expect("a wrapped cause is present");
        assert_eq!(cause.to_string(), "boom: disk");
    }

    #[test]
    fn outcome_constructors() {
        assert_eq!(Outcome::completed(), Outcome::Completed { note: None });
        assert_eq!(
            Outcome::completed_with("done"),
            Outcome::Completed {
                note: Some("done".into())
            }
        );
        assert_eq!(
            Outcome::pause_in(Duration::from_secs(5)),
            Outcome::Pause {
                resume_in: Duration::from_secs(5),
                note: None
            }
        );
    }
}
