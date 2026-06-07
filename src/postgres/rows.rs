//! Mapping `tokio_postgres::Row`s into the store's value types.

use crate::error::Error;
use crate::store::{JobRecord, JournalOutcome, JournalRecord, Status};
use tokio_postgres::Row;
use ulid::Ulid;

/// The `{{prefix}}_jobs` columns selected by claim and history queries, in a
/// fixed order so [`job_from_row`] can read them positionally.
pub(crate) const JOB_COLUMNS: &str = "id, kind, payload, priority, status, created_at, visible_at, \
     claim_expires_at, claimed_by, finished_at, run_count, failure_count, carry, dedup_key";

/// Build a [`JobRecord`] from a row whose columns are [`JOB_COLUMNS`] in order.
pub(crate) fn job_from_row(row: &Row) -> Result<JobRecord, Error> {
    let id: String = row.get(0);
    let id = Ulid::from_string(&id)
        .map_err(|_| Error::Config(format!("stored job id {id:?} is not a valid ULID")))?;

    let status: String = row.get(4);
    let status = Status::from_db(&status)
        .ok_or_else(|| Error::Config(format!("stored job status {status:?} is unknown")))?;

    Ok(JobRecord {
        id,
        kind: row.get(1),
        payload: row.get(2),
        priority: row.get(3),
        status,
        created_at: row.get(5),
        visible_at: row.get(6),
        claim_expires_at: row.get(7),
        claimed_by: row.get(8),
        finished_at: row.get(9),
        run_count: row.get(10),
        failure_count: row.get(11),
        carry: row.get(12),
        dedup_key: row.get(13),
    })
}

/// The `{{prefix}}_journal` columns selected by journal queries, in a fixed
/// order so [`journal_from_row`] can read them positionally.
pub(crate) const JOURNAL_COLUMNS: &str =
    "id, job_id, kind, run_no, recorded_at, outcome, note, attachment";

/// Build a [`JournalRecord`] from a row whose columns are [`JOURNAL_COLUMNS`] in
/// order.
pub(crate) fn journal_from_row(row: &Row) -> Result<JournalRecord, Error> {
    let job_id: String = row.get(1);
    let job_id = Ulid::from_string(&job_id)
        .map_err(|_| Error::Config(format!("journal job_id {job_id:?} is not a valid ULID")))?;

    let outcome: String = row.get(5);
    let outcome = JournalOutcome::from_db(&outcome)
        .ok_or_else(|| Error::Config(format!("journal outcome {outcome:?} is unknown")))?;

    Ok(JournalRecord {
        id: row.get(0),
        job_id,
        kind: row.get(2),
        run_no: row.get(3),
        recorded_at: row.get(4),
        outcome,
        note: row.get(6),
        attachment: row.get(7),
    })
}
