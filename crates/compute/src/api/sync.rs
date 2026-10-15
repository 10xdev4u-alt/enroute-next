//! `SyncService`: the compute's git client speaks to the remote, from one
//! snapshot.

use tonic::{Code, Request, Response, Status};

use enroute_api::api::v1alpha1::sync_service_server::SyncService;
use enroute_api::api::v1alpha1::{
    PushToRemoteRequest, PushToRemoteResponse, RefPushOutcome, RefSpec as WireRefSpec,
    Remote as WireRemote, ref_push_outcome::Status as WirePushStatus, remote::Credentials,
};

use crate::contract::wire::wire_oid_or_unset;
use crate::remote::{self, Error};

use super::Api;

#[tonic::async_trait]
impl SyncService for Api {
    #[tracing::instrument(name = "enroute::grpc::push_to_remote", skip(self, request))]
    async fn push_to_remote(
        &self,
        request: Request<PushToRemoteRequest>,
    ) -> Result<Response<PushToRemoteResponse>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let push = parse_push(request)?;
        let outcomes = self
            .remotes
            .push(&snap, &push)
            .await
            .map_err(|error| push_failed(&error))?;
        Ok(Response::new(PushToRemoteResponse {
            outcomes: outcomes.iter().map(to_wire_push_outcome).collect(),
        }))
    }
}

/// The push a call asks for.
fn parse_push(request: PushToRemoteRequest) -> Result<remote::PushRequest, Status> {
    let remote = request
        .remote
        .ok_or_else(|| Status::invalid_argument("no remote given"))?;

    Ok(remote::PushRequest {
        remote: parse_remote(remote),
        refs: request.refs.into_iter().map(parse_refspec).collect(),
        atomic: request.atomic,
    })
}

fn parse_remote(remote: WireRemote) -> remote::Remote {
    remote::Remote {
        url: remote.url,
        credentials: remote.credentials.map(|credentials| match credentials {
            Credentials::Basic(basic) => remote::Credentials::Basic {
                username: basic.username,
                password: basic.password,
            },
            Credentials::BearerToken(token) => remote::Credentials::Bearer(token),
        }),
    }
}

fn parse_refspec(spec: WireRefSpec) -> remote::RefSpec {
    remote::RefSpec {
        source: spec.source,
        destination: spec.destination,
        force: spec.force,
    }
}

/// One ref's outcome on the remote.
///
/// A ref the remote refused is data, not a failure: a push can land some refs
/// and be refused others.
fn to_wire_push_outcome(outcome: &remote::RefOutcome) -> RefPushOutcome {
    let (status, message) = match outcome.status {
        remote::PushStatus::Updated => (WirePushStatus::Updated, String::new()),
        remote::PushStatus::Deleted => (WirePushStatus::Deleted, String::new()),
        remote::PushStatus::UpToDate => (WirePushStatus::UpToDate, String::new()),
        remote::PushStatus::Rejected(ref why) => (WirePushStatus::Rejected, why.clone()),
    };
    RefPushOutcome {
        destination: outcome.destination.clone(),
        old_object_id: wire_oid_or_unset(outcome.old_id),
        new_object_id: wire_oid_or_unset(outcome.new_id),
        status: status.into(),
        message,
    }
}

/// A push that never got as far as an outcome.
///
/// The remote's own words reach the caller, who holds the relationship with
/// it — unlike a storage failure, which [`super::internal`] keeps.
fn push_failed(error: &Error) -> Status {
    let code = match error {
        Error::Request(_) => Code::InvalidArgument,
        Error::Remote(_) | Error::Protocol(_) | Error::Unsupported(_) => Code::FailedPrecondition,
        Error::Unauthorized => Code::PermissionDenied,
        Error::PackRefused(_) => Code::Aborted,
        // `Unknown` on purpose: the push reached the remote, so what landed
        // is not something this call can say either way any more.
        Error::ReportUnread(_) => Code::Unknown,
        Error::Transport(_) => Code::Unavailable,
        Error::Storage(_) => {
            tracing::error!(%error, "push to remote failed");
            return Status::internal("internal error");
        }
    };
    Status::new(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use enroute_api::api::v1alpha1::BasicAuth;

    use super::*;
    use crate::git::oid::Oid;

    fn request() -> PushToRemoteRequest {
        PushToRemoteRequest {
            remote: Some(WireRemote {
                url: "https://git.example.com/acme/widgets.git".into(),
                credentials: Some(Credentials::Basic(BasicAuth {
                    username: "x-access-token".into(),
                    password: "token".into(),
                })),
            }),
            refs: vec![WireRefSpec {
                source: "refs/heads/main".into(),
                destination: "refs/heads/main".into(),
                force: true,
            }],
            atomic: true,
            ..Default::default()
        }
    }

    /// A push names the remote it goes to: without one there is nothing to
    /// dial, and the caller hears it as their own request being wrong.
    #[test]
    fn a_push_without_a_remote_is_refused() {
        let refused = parse_push(PushToRemoteRequest {
            remote: None,
            ..request()
        })
        .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument);
    }

    /// Every field of the call reaches the client as it was sent.
    #[test]
    fn the_call_becomes_the_push_it_asks_for() {
        let push = parse_push(request()).unwrap();
        assert_eq!(push.remote.url, "https://git.example.com/acme/widgets.git");
        assert!(push.atomic);
        assert_eq!(push.refs.len(), 1);
        let spec = push.refs.first().expect("one ref");
        assert_eq!(spec.source, "refs/heads/main");
        assert_eq!(spec.destination, "refs/heads/main");
        assert!(spec.force);
        let Some(remote::Credentials::Basic { username, password }) = push.remote.credentials
        else {
            panic!("the basic credentials did not survive the call")
        };
        assert_eq!(
            (username.as_str(), password.as_str()),
            ("x-access-token", "token")
        );
    }

    /// The two kinds of proof stay the kinds they were sent as.
    #[test]
    fn both_kinds_of_credentials_cross() {
        let bearer = parse_push(PushToRemoteRequest {
            remote: Some(WireRemote {
                url: "https://git.example.com/r.git".into(),
                credentials: Some(Credentials::BearerToken("token".into())),
            }),
            ..request()
        })
        .unwrap();
        assert!(matches!(
            bearer.remote.credentials,
            Some(remote::Credentials::Bearer(token)) if token == "token"
        ));
        // A call that carries nothing but the URL.
        let anonymous = parse_push(PushToRemoteRequest {
            remote: Some(WireRemote {
                url: "https://git.example.com/r.git".into(),
                credentials: None,
            }),
            ..Default::default()
        })
        .unwrap();
        assert!(anonymous.remote.credentials.is_none());
        assert!(
            !anonymous.atomic,
            "a request that says nothing is not atomic"
        );
        assert!(anonymous.refs.is_empty());
    }

    /// A ref the remote refused is data: the push can land others, so it is
    /// reported per ref with the remote's own words.
    #[test]
    fn a_refused_ref_is_an_outcome_not_a_failure() {
        let outcome = |status| {
            to_wire_push_outcome(&remote::RefOutcome {
                destination: "refs/heads/main".into(),
                old_id: Oid([1; 20]),
                new_id: Oid([2; 20]),
                status,
            })
        };
        assert_eq!(
            outcome(remote::PushStatus::Updated).status,
            i32::from(WirePushStatus::Updated)
        );
        assert_eq!(
            outcome(remote::PushStatus::Deleted).status,
            i32::from(WirePushStatus::Deleted)
        );
        assert_eq!(
            outcome(remote::PushStatus::UpToDate).status,
            i32::from(WirePushStatus::UpToDate)
        );
        let refused = outcome(remote::PushStatus::Rejected("denied by hook".into()));
        assert_eq!(refused.status, i32::from(WirePushStatus::Rejected));
        assert_eq!(refused.message, "denied by hook");
        assert!(
            outcome(remote::PushStatus::Updated).message.is_empty(),
            "a status that is not a refusal carries no message"
        );
    }

    /// An id the remote does not hold is unset rather than all zeros: the
    /// contract tells the two apart, and a delete has no new id at all.
    #[test]
    fn an_absent_id_is_unset() {
        let deleted = to_wire_push_outcome(&remote::RefOutcome {
            destination: "refs/heads/gone".into(),
            old_id: Oid([3; 20]),
            new_id: Oid::ZERO,
            status: remote::PushStatus::Deleted,
        });
        assert!(deleted.new_object_id.is_none());
        assert_eq!(
            deleted.old_object_id.as_ref().map(|id| id.hex.clone()),
            Some("03".repeat(20))
        );
    }

    /// A push that never reached an outcome is a failure, and the code says
    /// who has to do something about it. Two answers are deliberate: the
    /// remote's own words reach the caller, and a storage failure does not.
    #[test]
    fn a_failure_before_an_outcome_names_its_code() {
        let cases: Vec<(Error, Code)> = vec![
            (Error::Request("no refs".into()), Code::InvalidArgument),
            (
                Error::Remote("no such repository".into()),
                Code::FailedPrecondition,
            ),
            (
                Error::Unsupported("atomic".into()),
                Code::FailedPrecondition,
            ),
            (
                Error::Protocol("bad pkt-line".into()),
                Code::FailedPrecondition,
            ),
            (Error::Unauthorized, Code::PermissionDenied),
            (Error::PackRefused("unpacker error".into()), Code::Aborted),
            (
                Error::ReportUnread("nothing came back".into()),
                Code::Unknown,
            ),
            (
                Error::Transport(
                    reqwest::Client::new()
                        .post("http://[")
                        .build()
                        .expect_err("a malformed URL"),
                ),
                Code::Unavailable,
            ),
        ];
        for (error, code) in cases {
            let refused = push_failed(&error);
            assert_eq!(refused.code(), code, "{error}");
            assert_eq!(refused.message(), error.to_string(), "{error}");
        }
        // The remote's words are for the caller; a storage failure's are not.
        let storage = push_failed(&Error::Storage(anyhow::anyhow!("bucket is gone")));
        assert_eq!(storage.code(), Code::Internal);
        assert_eq!(storage.message(), "internal error");
    }
}
