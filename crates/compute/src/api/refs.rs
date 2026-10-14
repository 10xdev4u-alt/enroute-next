//! `RefService`.

use std::time::{Duration, UNIX_EPOCH};

use tonic::{Request, Response, Status};

use enroute_api::api::v1alpha1::ref_service_server::RefService;
use enroute_api::api::v1alpha1::ref_update_outcome::Rejection;
use enroute_api::api::v1alpha1::ref_update_outcome::rejection::Reason;
use enroute_api::api::v1alpha1::{
    IsAncestorRequest, IsAncestorResponse, ListRefsRequest, ListRefsResponse, Ref,
    RefUpdateOutcome, UpdateRefsRequest, UpdateRefsResponse,
};

use crate::access::Hidden;
use crate::contract::wire::{optional_oid, require_oid, wire_oid, wire_system_time};
use crate::query::{self, RefRejection, RefView};

use super::Api;

#[tonic::async_trait]
impl RefService for Api {
    #[tracing::instrument(name = "enroute::grpc::list_refs", skip(self, request))]
    async fn list_refs(
        &self,
        request: Request<ListRefsRequest>,
    ) -> Result<Response<ListRefsResponse>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        // The API's caller is the application, which sees every ref.
        let view = RefView::new(&snap.meta, &Hidden::default(), &request.prefixes);
        let refs = view
            .refs
            .into_iter()
            .map(|(name, oid)| Ref {
                name: name.to_string(),
                object_id: Some(wire_oid(oid)),
                updated_at: wire_system_time(
                    snap.meta
                        .ref_updated_ms
                        .get(name)
                        .map(|ms| UNIX_EPOCH + Duration::from_millis(*ms)),
                ),
            })
            .collect();
        Ok(Response::new(ListRefsResponse {
            refs,
            default_branch: snap.meta.head.clone(),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::update_refs", skip(self, request))]
    async fn update_refs(
        &self,
        request: Request<UpdateRefsRequest>,
    ) -> Result<Response<UpdateRefsResponse>, Status> {
        let request = request.into_inner();
        // A missing repository wins over a malformed update.
        let (key, snap) = self.snapshot(request.repo.as_ref()).await?;
        let moves = request
            .updates
            .iter()
            .map(|update| {
                Ok(query::RefMove {
                    refname: update.refname.clone(),
                    old: optional_oid(update.old_object_id.as_ref())?,
                    new: optional_oid(update.new_object_id.as_ref())?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let outcomes = query::update_refs(&self.compute, key.as_str(), snap, &moves).await?;
        Ok(Response::new(UpdateRefsResponse {
            outcomes: outcomes
                .into_iter()
                .map(|outcome| RefUpdateOutcome {
                    refname: outcome.refname,
                    rejection: outcome.rejection.map(|why| Rejection {
                        reason: match why {
                            RefRejection::NonFastForward => Reason::NonFastForward,
                            RefRejection::AlreadyExists => Reason::AlreadyExists,
                            RefRejection::UnknownCommit => Reason::MissingObjects,
                            RefRejection::InvalidRefname => Reason::InvalidRefname,
                        }
                        .into(),
                        message: String::new(),
                    }),
                })
                .collect(),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::is_ancestor", skip(self, request))]
    async fn is_ancestor(
        &self,
        request: Request<IsAncestorRequest>,
    ) -> Result<Response<IsAncestorResponse>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let ancestor = require_oid(request.ancestor_commit_id.as_ref(), "ancestor_commit_id")?;
        let descendant = require_oid(
            request.descendant_commit_id.as_ref(),
            "descendant_commit_id",
        )?;
        let is_ancestor = query::is_ancestor(&snap, ancestor, descendant)
            .await
            .map_err(query::Error::from)?;
        Ok(Response::new(IsAncestorResponse { is_ancestor }))
    }
}
