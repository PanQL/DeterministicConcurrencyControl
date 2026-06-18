use crate::convert::{
    batch_from_proto, fs_op_from_proto, inode_entries_to_proto, read_entries_from_proto,
    tx_result_records_to_proto,
};
use crate::engine::{LocalReadResult, SequencerRuntime, ShardRuntime};
use crate::proto::pb;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct ShardService {
    runtime: Arc<ShardRuntime>,
}

impl ShardService {
    pub fn new(runtime: Arc<ShardRuntime>) -> Self {
        Self { runtime }
    }
}

#[tonic::async_trait]
impl pb::shard_server::Shard for ShardService {
    async fn ping(
        &self,
        _request: Request<pb::PingRequest>,
    ) -> std::result::Result<Response<pb::PingResponse>, Status> {
        Ok(Response::new(pb::PingResponse {
            node_id: self.runtime.node_id().to_string(),
            shard_id: self.runtime.shard_id(),
        }))
    }

    async fn execute_batch(
        &self,
        request: Request<pb::ExecuteBatchRequest>,
    ) -> std::result::Result<Response<pb::ExecuteBatchResponse>, Status> {
        let request = request.into_inner();
        let batch = request
            .batch
            .ok_or_else(|| Status::invalid_argument("missing batch"))
            .and_then(|batch| batch_from_proto(batch).map_err(Status::from))?;
        let summary = self
            .runtime
            .execute_batch(batch)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(pb::ExecuteBatchResponse {
            batch_id: summary.batch_id,
            shard_id: summary.shard_id,
            tx_results: tx_result_records_to_proto(&summary.tx_results),
        }))
    }

    async fn local_read_result(
        &self,
        request: Request<pb::LocalReadResultRequest>,
    ) -> std::result::Result<Response<pb::LocalReadResultResponse>, Status> {
        let request = request.into_inner();
        let reads = read_entries_from_proto(request.reads).map_err(Status::from)?;
        self.runtime
            .route_local_read_result(LocalReadResult {
                batch_id: request.batch_id,
                tx_id: request.tx_id,
                from_shard: request.from_shard,
                reads,
            })
            .await
            .map_err(Status::from)?;
        Ok(Response::new(pb::LocalReadResultResponse {}))
    }

    async fn dump_state(
        &self,
        _request: Request<pb::DumpStateRequest>,
    ) -> std::result::Result<Response<pb::DumpStateResponse>, Status> {
        let state = self.runtime.dump_state().map_err(Status::from)?;
        Ok(Response::new(pb::DumpStateResponse {
            entries: inode_entries_to_proto(&state),
        }))
    }
}

pub struct SequencerService {
    runtime: Arc<SequencerRuntime>,
}

impl SequencerService {
    pub fn new(runtime: Arc<SequencerRuntime>) -> Self {
        Self { runtime }
    }
}

#[tonic::async_trait]
impl pb::sequencer_server::Sequencer for SequencerService {
    async fn ping(
        &self,
        _request: Request<pb::PingRequest>,
    ) -> std::result::Result<Response<pb::PingResponse>, Status> {
        Ok(Response::new(pb::PingResponse {
            node_id: self.runtime.node_id().to_string(),
            shard_id: 0,
        }))
    }

    async fn submit_batch(
        &self,
        request: Request<pb::SubmitBatchRequest>,
    ) -> std::result::Result<Response<pb::SubmitBatchResponse>, Status> {
        let request = request.into_inner();
        let mut ops = Vec::with_capacity(request.ops.len());
        for op in request.ops {
            ops.push(fs_op_from_proto(Some(op)).map_err(Status::from)?);
        }
        let summary = self.runtime.submit_ops(ops).await.map_err(Status::from)?;
        Ok(Response::new(pb::SubmitBatchResponse {
            batch_id: summary.batch_id,
            tx_ids: summary.tx_ids,
            tx_results: tx_result_records_to_proto(&summary.tx_results),
        }))
    }
}

pub fn shard_service(runtime: Arc<ShardRuntime>) -> ShardService {
    ShardService::new(runtime)
}

pub fn sequencer_service(runtime: Arc<SequencerRuntime>) -> SequencerService {
    SequencerService::new(runtime)
}
