//! The contract, `enroute.api.v1alpha1`, served from a compute.
//!
//! The wire layer over [`crate::query`] and the compute's lifecycle calls: one
//! module per service, each turning its results into messages.
//! Nothing here authenticates, the application being the only caller: whether
//! it may reach this port is the deployment's to answer — see
//! docs/operate/security.md.

mod commit;
mod objects;
mod refs;
mod repository;
mod sync;

use std::pin::Pin;
use std::sync::Arc;

use tonic::Status;

use enroute_api::api::v1alpha1::commit_service_server::CommitServiceServer;
use enroute_api::api::v1alpha1::object_service_server::ObjectServiceServer;
use enroute_api::api::v1alpha1::ref_service_server::RefServiceServer;
use enroute_api::api::v1alpha1::repository_service_server::RepositoryServiceServer;
use enroute_api::api::v1alpha1::sync_service_server::SyncServiceServer;
use enroute_api::common::v1alpha1::RepoKey as WireRepoKey;

use crate::compute::Compute;
use crate::query;
use crate::remote;
use crate::repo::view::Snapshot;

use crate::contract::repo_key::RepoKey;
use crate::contract::wire::parse_repo;

/// A server-streamed response.
type ResponseStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send>>;

/// Everything the contract is served out of.
#[derive(Clone)]
struct Api {
    compute: Arc<Compute>,
    remotes: Arc<remote::Client>,
}

impl std::fmt::Debug for Api {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api").finish_non_exhaustive()
    }
}

impl Api {
    /// The repository a call names, as of every write committed before the
    /// call: a caller reads what it wrote.
    async fn snapshot(
        &self,
        repo: Option<&WireRepoKey>,
    ) -> Result<(RepoKey, Arc<Snapshot>), Status> {
        let key = parse_repo(repo)?;
        let snap = query::snapshot(&self.compute, key.as_str()).await?;
        Ok((key, snap))
    }
}

/// The status a caller gets for an error.
impl From<query::Error> for Status {
    fn from(error: query::Error) -> Self {
        match error {
            query::Error::NotFound | query::Error::Missing(_) => {
                Status::not_found(error.to_string())
            }
            query::Error::WrongKind { .. } | query::Error::Invalid(_) => {
                Status::invalid_argument(error.to_string())
            }
            query::Error::Moved(_) => Status::failed_precondition(error.to_string()),
            query::Error::Contended(_) => Status::aborted(error.to_string()),
            query::Error::Failed(error) => internal(&error),
        }
    }
}

/// A finished answer, as a server stream.
fn streamed<T: Send + 'static>(messages: Vec<T>) -> ResponseStream<T> {
    Box::pin(futures::stream::iter(messages.into_iter().map(Ok)))
}

/// The contract's services over `compute`, with reflection, ready to mount.
///
/// A sync pushes with `remotes`.
///
/// # Errors
///
/// If the compiled-in descriptors do not resolve, which is a build fault.
pub fn router(
    compute: Arc<Compute>,
    remotes: remote::Client,
) -> Result<tonic::service::Routes, tonic_reflection::server::Error> {
    let api = Api {
        compute,
        remotes: Arc::new(remotes),
    };
    let reflection = || {
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(enroute_api::FILE_DESCRIPTOR_SET)
    };
    Ok(
        tonic::service::Routes::new(RefServiceServer::new(api.clone()))
            .add_service(RepositoryServiceServer::new(api.clone()))
            .add_service(ObjectServiceServer::new(api.clone()))
            .add_service(SyncServiceServer::new(api.clone()))
            .add_service(
                CommitServiceServer::new(api).max_decoding_message_size(commit::MAX_REQUEST_BYTES),
            )
            .add_service(reflection().build_v1()?)
            .add_service(reflection().build_v1alpha()?),
    )
}

/// Engine failures reach a caller as `Internal` with no detail: what went
/// wrong inside is not theirs to act on, and the log carries it.
fn internal(error: &anyhow::Error) -> Status {
    tracing::error!(error = format!("{error:#}"), "grpc call failed");
    Status::internal("internal error")
}

#[cfg(test)]
mod reflection {
    use prost::Message as _;

    /// Both reflection services resolve the descriptors compiled into them.
    ///
    /// The one way this fails is a build that produced a set with a dangling
    /// import, which nothing else here would notice.
    #[test]
    fn the_services_build() {
        let configure = || {
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(enroute_api::FILE_DESCRIPTOR_SET)
        };

        configure().build_v1().expect("the v1 reflection service");
        configure()
            .build_v1alpha()
            .expect("the v1alpha reflection service");
    }

    /// The hook contract is in the descriptors, though it declares no service.
    ///
    /// Reflection walks services to their imports and would never reach it,
    /// so an integrator can ask for it only because it is registered here.
    #[test]
    fn the_descriptors_carry_the_hook_contract() {
        let set = prost_types::FileDescriptorSet::decode(enroute_api::FILE_DESCRIPTOR_SET)
            .expect("the compiled-in descriptors");
        let files: Vec<&str> = set.file.iter().filter_map(|f| f.name.as_deref()).collect();

        for wanted in [
            "enroute/common/v1alpha1/common.proto",
            "enroute/api/v1alpha1/repository.proto",
            "enroute/api/v1alpha1/ref.proto",
            "enroute/api/v1alpha1/object.proto",
            "enroute/api/v1alpha1/sync.proto",
            "enroute/api/v1alpha1/commit.proto",
            "enroute/hook/v1alpha1/hook.proto",
        ] {
            assert!(
                files.contains(&wanted),
                "{wanted} is not in the descriptors"
            );
        }
    }
}
