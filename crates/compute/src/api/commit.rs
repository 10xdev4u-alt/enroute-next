//! `CommitService`: [`query::create_commit`] on the wire.

use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};

use enroute_api::api::v1alpha1::commit_service_server::CommitService;
use enroute_api::api::v1alpha1::{
    CreateCommitRequest, CreateCommitResponse, ExpectedOldCommit, FileMode as WireFileMode,
    Identity as WireIdentity, PathChange as WirePathChange, expected_old_commit::Expectation,
    path_change,
};

use crate::contract::wire::{parse_oid, parse_repo, wire_oid};
use crate::query::{self, Expected, FileMode, Identity, NewCommit, PathChange, PathEdit};

use super::{Api, internal};

/// The most a request may be, encoded.
///
/// A commit is held in memory while its objects are built, so the server
/// caps it, before it reads the request.
pub(super) const MAX_REQUEST_BYTES: usize = 32 << 20;

#[tonic::async_trait]
impl CommitService for Api {
    #[tracing::instrument(name = "enroute::grpc::create_commit", skip(self, request))]
    async fn create_commit(
        &self,
        request: Request<CreateCommitRequest>,
    ) -> Result<Response<CreateCommitResponse>, Status> {
        let request = request.into_inner();
        let key = parse_repo(request.repo.as_ref())?;
        let commit = parse_commit(request, now()?)?;
        let created = query::create_commit(&self.compute, key.as_str(), &commit).await?;
        Ok(Response::new(CreateCommitResponse {
            commit_id: Some(wire_oid(created.commit)),
            tree_id: Some(wire_oid(created.tree)),
            parent_commit_id: created.parent.map(wire_oid),
        }))
    }
}

/// Seconds since the epoch, by this node's clock.
fn now() -> Result<i64, Status> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_secs()).ok())
        .ok_or_else(|| internal(&anyhow::anyhow!("the clock is before 1970")))
}

/// The commit a call asks for, with its defaults filled in.
fn parse_commit(request: CreateCommitRequest, now: i64) -> Result<NewCommit, Status> {
    let author = request
        .author
        .ok_or_else(|| Status::invalid_argument("author is required"))?;
    let author = identity(author, now);
    let committer = request
        .committer
        .map_or_else(|| author.clone(), |who| identity(who, now));
    Ok(NewCommit {
        branch: request.branch,
        expected: expected(request.expected_old_commit)?,
        message: request.message,
        author,
        committer,
        changes: request
            .changes
            .into_iter()
            .map(change)
            .collect::<Result<_, _>>()?,
    })
}

/// An identity, timed `now` when it names no time.
///
/// Git counts whole seconds, so nanoseconds are dropped.
fn identity(who: WireIdentity, now: i64) -> Identity {
    Identity {
        name: who.name.into_bytes(),
        email: who.email.into_bytes(),
        seconds: who.timestamp.map_or(now, |at| at.seconds),
        offset_seconds: who.utc_offset_seconds,
    }
}

fn expected(expected: Option<ExpectedOldCommit>) -> Result<Expected, Status> {
    let Some(expected) = expected else {
        return Ok(Expected::Any);
    };
    match expected.expectation {
        Some(Expectation::Absent(_)) => Ok(Expected::Absent),
        Some(Expectation::CommitId(id)) => {
            let oid = parse_oid(&id)?;
            if oid.is_zero() {
                return Err(Status::invalid_argument(
                    "expected_old_commit.commit_id is the zero id: set absent instead",
                ));
            }
            Ok(Expected::At(oid))
        }
        None => Err(Status::invalid_argument(
            "expected_old_commit names neither a commit nor absent",
        )),
    }
}

fn change(change: WirePathChange) -> Result<PathChange, Status> {
    let edit = match change.change {
        Some(path_change::Change::Put(file)) => PathEdit::Put {
            mode: match WireFileMode::try_from(file.mode) {
                Ok(WireFileMode::Unspecified | WireFileMode::Regular) => FileMode::Regular,
                Ok(WireFileMode::Executable) => FileMode::Executable,
                Ok(WireFileMode::Symlink) => FileMode::Symlink,
                Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "{}: no file mode {}",
                        change.path, file.mode
                    )));
                }
            },
            content: file.content,
        },
        Some(path_change::Change::Delete(_)) => PathEdit::Delete,
        None => {
            return Err(Status::invalid_argument(format!(
                "{}: neither put nor delete",
                change.path
            )));
        }
    };
    Ok(PathChange {
        path: change.path,
        edit,
    })
}

#[cfg(test)]
mod tests {
    use enroute_api::api::v1alpha1::{DeleteFile, PutFile};
    use enroute_api::common::v1alpha1::ObjectId;

    use super::*;

    fn request() -> CreateCommitRequest {
        CreateCommitRequest {
            branch: "refs/heads/main".into(),
            author: Some(WireIdentity {
                name: "Ada".into(),
                email: "ada@example.com".into(),
                timestamp: None,
                utc_offset_seconds: 3600,
            }),
            changes: vec![WirePathChange {
                path: "a".into(),
                change: Some(path_change::Change::Delete(DeleteFile {})),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn the_committer_is_the_author_at_the_time_of_the_call() {
        let commit = parse_commit(request(), 1_700_000_000).unwrap();
        assert_eq!(commit.author.seconds, 1_700_000_000);
        assert_eq!(commit.committer, commit.author);
        assert_eq!(commit.expected, Expected::Any);
    }

    #[test]
    fn an_author_is_required() {
        let refused = parse_commit(
            CreateCommitRequest {
                author: None,
                ..request()
            },
            0,
        )
        .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn an_expectation_names_a_commit_or_absence() {
        let at = |hex: &str| {
            Some(ExpectedOldCommit {
                expectation: Some(Expectation::CommitId(ObjectId { hex: hex.into() })),
            })
        };
        assert!(matches!(expected(at(&"1".repeat(40))), Ok(Expected::At(_))));
        expected(at(&"0".repeat(40))).unwrap_err();
        expected(Some(ExpectedOldCommit { expectation: None })).unwrap_err();
        assert_eq!(
            expected(Some(ExpectedOldCommit {
                expectation: Some(Expectation::Absent(
                    enroute_api::api::v1alpha1::expected_old_commit::Absent {}
                )),
            }))
            .unwrap(),
            Expected::Absent
        );
    }

    #[test]
    fn an_unspecified_mode_is_a_regular_file_and_an_unknown_one_is_refused() {
        let put = |mode: i32| WirePathChange {
            path: "a".into(),
            change: Some(path_change::Change::Put(PutFile {
                content: bytes::Bytes::new(),
                mode,
            })),
        };
        assert!(matches!(
            change(put(0)).unwrap().edit,
            PathEdit::Put {
                mode: FileMode::Regular,
                ..
            }
        ));
        change(put(42)).unwrap_err();
    }
}
