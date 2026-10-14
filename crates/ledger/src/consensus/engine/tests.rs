//! The engine and its two stores: what a restart finds when they disagree,
//! and what keeps the log from being truncated too far.

use std::path::Path;

use enroute_shared::oid::Oid;
use enroute_shared::state::{Commit, LayerMeta, RefUpdate, layer_id};

use crate::state_store::Root;

use super::*;

/// A standalone node on `dir`, compacting every `every` entries.
fn start(dir: &Path, every: u64) -> Result<(Arc<Engine>, u64)> {
    let db = crate::logdb::open(&dir.join("log"))?;
    let id = crate::logdb::store_id(&db)?;
    let state = Arc::new(StateStore::open(&dir.join("state"), 16 << 20)?);
    Ok((
        Engine::start(db, state, every, id, None, mpsc::unbounded_channel().0)?,
        id,
    ))
}

/// Stop a node, and wait for it to close its stores.
async fn stop(engine: Arc<Engine>) {
    engine.shutdown();
    tokio::time::timeout(Duration::from_secs(5), engine.stopped())
        .await
        .expect("the engine did not stop");
}

/// Wait (up to 5 s) until `f` holds. A restarted solo node elects itself
/// only after an election timeout of up to 2 s.
async fn wait_until(mut f: impl FnMut() -> bool) -> bool {
    for _ in 0..250 {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    f()
}

fn push(name: &str, n: u8) -> Command {
    Command::commit(
        "r",
        Commit {
            updates: vec![RefUpdate {
                name: name.into(),
                old: None,
                new: Some(Oid([n; 20])),
            }],
            ..Default::default()
        },
    )
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to.join(e.file_name()));
        } else {
            std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

async fn propose(engine: &Engine, cmd: Command) -> Outcome {
    let out = engine.propose(cmd).await.unwrap();
    assert!(
        out.error.is_none() && out.ref_results.iter().all(|r| r.error.is_none()),
        "{out:?}"
    );
    out
}

/// A membership change is recorded with the state; replay at a restart
/// stops before one, and raft applies it (and what follows) again.
#[tokio::test]
async fn membership_changes_are_replayed_by_raft() {
    let dir = tempfile::tempdir().unwrap();
    let (e, id) = start(dir.path(), 1000).unwrap();
    e.host("r", 1, RepoMeta::new(), vec![id]).await.unwrap();
    propose(&e, push("refs/heads/a", 1)).await;
    stop(e).await;
    copy_dir(&dir.path().join("state"), &dir.path().join("old"));

    // A solo leader adds a learner alone, then takes a write; the state
    // store then loses both.
    let (e, _) = start(dir.path(), 1000).unwrap();
    e.set_members(vec![id, 99], Vec::new());
    let learners = |e: &Engine| {
        e.record("r")
            .unwrap()
            .map(|r| r.learners)
            .unwrap_or_default()
    };
    assert!(
        wait_until(|| learners(&e) == [99]).await,
        "the learner was not added"
    );
    propose(&e, push("refs/heads/b", 2)).await;
    stop(e).await;
    std::fs::remove_dir_all(dir.path().join("state")).unwrap();
    std::fs::rename(dir.path().join("old"), dir.path().join("state")).unwrap();

    let (e, _) = start(dir.path(), 1000).unwrap();
    assert!(
        wait_until(|| learners(&e) == [99] && e.repo_meta("r").is_some_and(|m| m.refs.len() == 2))
            .await
    );
    stop(e).await;
}

/// A state store that lost writes it had not synced (an old copy of it)
/// catches up from the log at restart.
#[tokio::test]
async fn restart_replays_the_log_after_the_state() {
    let dir = tempfile::tempdir().unwrap();
    let (e, id) = start(dir.path(), 1000).unwrap();
    e.host("r", 1, RepoMeta::new(), vec![id]).await.unwrap();
    for i in 0..3 {
        propose(&e, push(&format!("refs/heads/a{i}"), i)).await;
    }
    stop(e).await;
    copy_dir(&dir.path().join("state"), &dir.path().join("old"));

    let (e, _) = start(dir.path(), 1000).unwrap();
    for i in 0..3 {
        propose(&e, push(&format!("refs/heads/b{i}"), i)).await;
    }
    let want = e.repo_meta("r").unwrap();
    assert_eq!(want.refs.len(), 6);
    stop(e).await;

    std::fs::remove_dir_all(dir.path().join("state")).unwrap();
    std::fs::rename(dir.path().join("old"), dir.path().join("state")).unwrap();
    let (e, _) = start(dir.path(), 1000).unwrap();
    assert_eq!(e.repo_meta("r").unwrap(), want);
    assert_eq!(e.hosted("r").unwrap().version, want.version);
    propose(&e, push("refs/heads/c", 9)).await;
    stop(e).await;
}

/// A snapshot installed in the state store whose log was not reset to it
/// (a crash in between): the restart resets the log, and the log goes on
/// from the snapshot.
#[tokio::test]
async fn restart_resets_a_log_behind_its_state() {
    let dir = tempfile::tempdir().unwrap();
    let (e, id) = start(dir.path(), 1000).unwrap();
    e.host("r", 1, RepoMeta::new(), vec![id]).await.unwrap();
    propose(&e, push("refs/heads/main", 1)).await;
    let log = e.hosted("r").unwrap().log;
    let mut meta = e.repo_meta("r").unwrap();
    stop(e).await;

    let (ahead, version) = {
        let state = StateStore::open(&dir.path().join("state"), 16 << 20).unwrap();
        let root = state.root(log).unwrap();
        meta.refs
            .insert("refs/heads/installed".into(), Oid([7; 20]));
        meta.version += 10;
        let ahead = Root {
            applied_index: root.applied_index + 10,
            ..root
        };
        state
            .install(log, ahead.clone(), stored::entries(&meta).unwrap())
            .unwrap();
        (ahead.applied_index, meta.version)
    };
    for _ in 0..2 {
        let (e, _) = start(dir.path(), 1000).unwrap();
        let h = e.hosted("r").unwrap();
        assert!(h.applied >= ahead, "{} < {ahead}", h.applied);
        assert!(
            e.repo_meta("r")
                .unwrap()
                .refs
                .contains_key("refs/heads/installed")
        );
        let out = propose(&e, push(&format!("refs/heads/after-{}", h.applied), 2)).await;
        assert!(out.repo_version > version);
        stop(e).await;
    }
}

/// A log truncated past what its state store kept cannot be replayed: the
/// node refuses to start rather than lose those entries silently.
#[tokio::test]
async fn a_state_behind_the_log_start_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (e, id) = start(dir.path(), 2).unwrap();
    e.host("r", 1, RepoMeta::new(), vec![id]).await.unwrap();
    propose(&e, push("refs/heads/main", 1)).await;
    let h = e.hosted("r").unwrap();
    let (log, old) = (h.log, h.applied);
    stop(e).await;
    copy_dir(&dir.path().join("state"), &dir.path().join("old"));

    let (e, _) = start(dir.path(), 2).unwrap();
    for i in 0..6 {
        propose(&e, push(&format!("refs/heads/b{i}"), i)).await;
    }
    // Truncated past the old state (once the new one is durable).
    assert!(
        wait_until(|| e
            .db
            .upgrade()
            .unwrap()
            .first_index(log)
            .is_none_or(|f| f > old + 1))
        .await
    );
    stop(e).await;
    std::fs::remove_dir_all(dir.path().join("state")).unwrap();
    std::fs::rename(dir.path().join("old"), dir.path().join("state")).unwrap();
    let err = start(dir.path(), 2).expect_err("refused");
    assert!(
        format!("{err:#}").contains("before the log's start"),
        "{err:#}"
    );
}

/// The log keeps an inline layer's entry until the layer is flushed, and
/// its bytes are read from there meanwhile.
#[tokio::test]
async fn inline_layers_hold_the_log_until_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let (e, id) = start(dir.path(), 1).unwrap();
    e.host("r", 1, RepoMeta::new(), vec![id]).await.unwrap();
    propose(&e, push("refs/heads/main", 1)).await;
    let inline = LayerMeta {
        id: "inline-1".into(),
        objects: 3,
        bytes: 3,
        data: Some(b"abc".to_vec().into()),
        ..Default::default()
    };
    propose(
        &e,
        Command::commit(
            "r",
            Commit {
                add_layers: vec![inline],
                ..Default::default()
            },
        ),
    )
    .await;
    let h = e.hosted("r").unwrap();
    let (log, pinned) = (h.log, h.applied);
    for i in 0..5 {
        propose(&e, push(&format!("refs/heads/b{i}"), i)).await;
    }
    stop(e).await;
    // The first entry the log keeps (none: every one was compacted).
    let first = |dir: &Path| {
        crate::logdb::open(&dir.join("log"))
            .unwrap()
            .first_index(log)
    };
    assert!(first(dir.path()).is_some());
    assert!(
        first(dir.path()).unwrap() <= pinned,
        "the inline layer's entry is kept"
    );

    // After a restart its bytes still come from the log.
    let (e, _) = start(dir.path(), 1).unwrap();
    let l = e.repo_meta("r").unwrap().layers[0].clone();
    assert_eq!(l.data.as_deref(), Some(&b"abc"[..]));
    let flushed = LayerMeta {
        id: layer_id(7, 0),
        objects: 3,
        bytes: 3,
        first_id: l.first_id,
        id_span: l.id_span,
        ..Default::default()
    };
    propose(
        &e,
        Command::commit(
            "r",
            Commit {
                add_layers: vec![flushed],
                remove_layers: vec![(&l).into()],
                ..Default::default()
            },
        ),
    )
    .await;
    propose(&e, push("refs/heads/after", 1)).await;
    assert!(
        e.repo_meta("r")
            .unwrap()
            .layers
            .iter()
            .all(LayerMeta::is_stored)
    );
    // Truncated once the flush is durable (the apply thread syncs and
    // reports every SYNC_EVERY).
    let moved_on = || {
        e.db.upgrade()
            .unwrap()
            .first_index(log)
            .is_none_or(|f| f > pinned)
    };
    assert!(wait_until(moved_on).await);
    stop(e).await;
    assert!(
        first(dir.path()).is_none_or(|f| f > pinned),
        "flushed, the log moves on"
    );
}
