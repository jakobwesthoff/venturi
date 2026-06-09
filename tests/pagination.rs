//! Integration tests for keyset history pagination: stable descending pages,
//! tie-breaking on equal `created_at`, the empty last page, and composition with
//! a column filter.
//!
//! Requires Docker; marked `#[ignore]`. Run with `just integration-test`.
//!
//! These drive the storage layer directly through `Store::enqueue`/`query_jobs`
//! rather than the worker, so each job's `created_at` and `id` are fixed by the
//! test. That control is what lets the tie-break case assert a deterministic
//! order across rows that share a `created_at`.

mod common;

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use common::TestDb;
use serde_json::json;
use ulid::Ulid;
use venturi::postgres::PostgresStore;
use venturi::store::{HistoryFilter, JobRecord, NewJob, Store};

/// A pending job at an exact enqueue time, with an empty payload and carry.
fn new_job(id: Ulid, kind: &str, created_at: DateTime<Utc>) -> NewJob {
    NewJob {
        id,
        kind: kind.to_owned(),
        payload: json!(null),
        priority: 1,
        created_at,
        visible_at: created_at,
        carry: json!(null),
        dedup_key: None,
    }
}

/// A fixed base instant plus `seconds`, so tests read in real time order.
fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + seconds, 0)
        .single()
        .expect("valid fixed timestamp")
}

/// Page through the whole history `limit` rows at a time, following the keyset
/// cursor, and return the rows in the order they were yielded.
async fn page_all(store: &Arc<PostgresStore>, kind: Option<&str>, limit: i64) -> Vec<JobRecord> {
    let mut collected: Vec<JobRecord> = Vec::new();
    let mut cursor: Option<(DateTime<Utc>, Ulid)> = None;
    loop {
        let page = store
            .query_jobs(&HistoryFilter {
                kind: kind.map(str::to_owned),
                created_before: cursor,
                limit: Some(limit),
                ..Default::default()
            })
            .await
            .expect("query page");
        let Some(last) = page.last() else {
            break;
        };
        cursor = Some((last.created_at, last.id));
        collected.extend(page);
    }
    collected
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn keyset_pages_in_created_desc_order_without_gaps_or_repeats() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);

    // Ten jobs at strictly increasing times, inserted out of order to prove the
    // ordering comes from the query, not the insert sequence.
    let mut ids = Vec::new();
    for seconds in [3, 7, 1, 9, 5, 0, 8, 2, 6, 4] {
        let id = Ulid::new();
        ids.push((at(seconds), id));
        store
            .enqueue(&new_job(id, "alpha", at(seconds)))
            .await
            .expect("enqueue");
    }

    // Expected: newest first, breaking ties by id desc (no ties here).
    let mut expected: Vec<(DateTime<Utc>, Ulid)> = ids.clone();
    expected.sort_by(|a, b| b.cmp(a));

    // A page size that does not divide the total exercises a short final page.
    let paged = page_all(&store, None, 3).await;
    let got: Vec<(DateTime<Utc>, Ulid)> = paged.iter().map(|r| (r.created_at, r.id)).collect();

    assert_eq!(
        got, expected,
        "paged order must be the full created_at desc, id desc order"
    );
    assert_eq!(got.len(), 10, "every row appears exactly once across pages");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn keyset_breaks_ties_on_equal_created_at_by_id_desc() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);

    // Five jobs that all share one created_at: only the id can order them.
    let shared = at(42);
    let mut ids: Vec<Ulid> = (0..5).map(|_| Ulid::new()).collect();
    for id in &ids {
        store
            .enqueue(&new_job(*id, "alpha", shared))
            .await
            .expect("enqueue");
    }

    // Walk one row per page so every step crosses the keyset on equal created_at.
    let paged = page_all(&store, None, 1).await;
    let got: Vec<Ulid> = paged.iter().map(|r| r.id).collect();

    ids.sort_by(|a, b| b.cmp(a)); // id desc
    assert_eq!(
        got, ids,
        "equal created_at rows must page in strict id desc order"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn keyset_past_the_oldest_row_returns_an_empty_page() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);

    let oldest_id = Ulid::new();
    store
        .enqueue(&new_job(oldest_id, "alpha", at(10)))
        .await
        .expect("enqueue");
    store
        .enqueue(&new_job(Ulid::new(), "alpha", at(20)))
        .await
        .expect("enqueue");

    // A cursor at the oldest row excludes it and everything before it: empty.
    let page = store
        .query_jobs(&HistoryFilter {
            created_before: Some((at(10), oldest_id)),
            limit: Some(50),
            ..Default::default()
        })
        .await
        .expect("query");
    assert!(page.is_empty(), "no row is older than the oldest cursor");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn keyset_composes_with_a_kind_filter() {
    let db = TestDb::start().await;
    let store = Arc::new(db.store("venturi").await);

    // Interleave two kinds in time; the cursor and the kind filter must both apply.
    let mut alpha_keys = Vec::new();
    for seconds in [1, 3, 5, 7, 9] {
        let id = Ulid::new();
        alpha_keys.push((at(seconds), id));
        store
            .enqueue(&new_job(id, "alpha", at(seconds)))
            .await
            .expect("enqueue");
    }
    for seconds in [2, 4, 6, 8] {
        store
            .enqueue(&new_job(Ulid::new(), "beta", at(seconds)))
            .await
            .expect("enqueue");
    }

    let mut expected = alpha_keys.clone();
    expected.sort_by(|a, b| b.cmp(a));

    let paged = page_all(&store, Some("alpha"), 2).await;
    let got: Vec<(DateTime<Utc>, Ulid)> = paged.iter().map(|r| (r.created_at, r.id)).collect();

    assert_eq!(
        got, expected,
        "filtered pages stay within the kind and in keyset order"
    );
    assert!(
        paged.iter().all(|r| r.kind == "alpha"),
        "no other kind leaks into the page"
    );
}
