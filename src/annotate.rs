use crate::storage::Store;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Transaction, TransactionBehavior};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageAnnotation {
    pub item_id: String,
    pub axis: String,
    pub score: u8,
    pub model: String,
    pub annotator_version: String,
    pub annotated_at: DateTime<Utc>,
}

pub fn insert_annotations(store: &Store, annotations: &[MessageAnnotation]) -> Result<()> {
    for annotation in annotations {
        if !(1..=5).contains(&annotation.score) {
            bail!(
                "annotation score for {}:{} must be between 1 and 5",
                annotation.item_id,
                annotation.axis
            );
        }
    }

    store.with_conn(|conn| {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
            .context("starting annotation batch")?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO message_annotations
                 (item_id, axis, score, model, annotator_version, annotated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for annotation in annotations {
                stmt.execute(params![
                    annotation.item_id,
                    annotation.axis,
                    annotation.score,
                    annotation.model,
                    annotation.annotator_version,
                    annotation.annotated_at.to_rfc3339(),
                ])?;
            }
        }
        tx.commit().context("committing annotation batch")
    })
}

pub fn has_annotation(
    store: &Store,
    item_id: &str,
    axis: &str,
    annotator_version: &str,
) -> Result<bool> {
    store.with_conn(|conn| {
        Ok(conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM message_annotations
                 WHERE item_id = ?1 AND axis = ?2 AND annotator_version = ?3
             )",
            params![item_id, axis, annotator_version],
            |row| row.get(0),
        )?)
    })
}

pub fn latest_annotations(store: &Store) -> Result<Vec<MessageAnnotation>> {
    let rows = store.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT item_id, axis, score, model, annotator_version, annotated_at
             FROM message_annotations
             WHERE annotator_version = (
                 SELECT annotator_version
                 FROM message_annotations
                 ORDER BY annotated_at DESC, annotator_version DESC
                 LIMIT 1
             )
             ORDER BY item_id, axis",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u8>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })?;

    rows.into_iter()
        .map(
            |(item_id, axis, score, model, annotator_version, annotated_at)| {
                Ok(MessageAnnotation {
                    item_id,
                    axis,
                    score,
                    model,
                    annotator_version,
                    annotated_at: DateTime::parse_from_rfc3339(&annotated_at)
                        .with_context(|| format!("parsing annotation timestamp {annotated_at}"))?
                        .with_timezone(&Utc),
                })
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn annotation(axis: &str, score: u8, version: &str, annotated_at: &str) -> MessageAnnotation {
        MessageAnnotation {
            item_id: "item_1".to_string(),
            axis: axis.to_string(),
            score,
            model: "test-model".to_string(),
            annotator_version: version.to_string(),
            annotated_at: DateTime::parse_from_rfc3339(annotated_at)
                .expect("timestamp")
                .with_timezone(&Utc),
        }
    }

    #[test]
    fn annotation_storage_is_versioned_resumable_and_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let first = vec![
            annotation("frustration", 2, "v1", "2026-07-11T00:00:00Z"),
            annotation("energy", 4, "v1", "2026-07-11T00:00:00Z"),
        ];

        insert_annotations(&store, &first).expect("insert first version");
        insert_annotations(&store, &first).expect("repeat first version");
        assert!(has_annotation(&store, "item_1", "energy", "v1").expect("check annotation"));
        assert!(!has_annotation(&store, "item_1", "energy", "v2").expect("check version"));

        let second = vec![
            annotation("energy", 5, "v2", "2026-07-12T00:00:00Z"),
            annotation("frustration", 3, "v2", "2026-07-12T00:00:00Z"),
        ];
        insert_annotations(&store, &second).expect("insert second version");

        let count = store
            .with_conn(|conn| {
                Ok(
                    conn.query_row("SELECT COUNT(*) FROM message_annotations", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                )
            })
            .expect("count annotations");
        assert_eq!(count, 4);
        assert_eq!(
            latest_annotations(&store).expect("latest annotations"),
            second
        );
    }

    #[test]
    fn annotation_scores_must_be_between_one_and_five() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open store");
        let invalid = annotation("energy", 0, "v1", "2026-07-12T00:00:00Z");

        let error = insert_annotations(&store, &[invalid]).expect_err("reject invalid score");
        assert!(error.to_string().contains("between 1 and 5"));
    }
}
