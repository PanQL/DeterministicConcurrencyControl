use crate::convert::{
    batch_from_proto, fs_op_from_proto, inode_entries_to_proto, local_read_status_from_i32,
    read_entries_from_proto, read_phase_from_i32, scc_reorder_records_to_proto,
    scheduler_profile_records_to_proto, tx_result_records_to_proto, tx_result_to_i32,
};
use crate::engine::{
    ClientTxResult, LocalReadResult, SccCompletionReport, SequencerRuntime, ShardRuntime,
};
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
            .ok_or_else(|| Status::invalid_argument("missing batch"))?;
        let batch = batch_from_proto(batch).map_err(Status::from)?;
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
                phase: read_phase_from_i32(request.phase).map_err(Status::from)?,
                from_shard: request.from_shard,
                status: local_read_status_from_i32(request.status).map_err(Status::from)?,
                reads,
            })
            .await
            .map_err(Status::from)?;
        Ok(Response::new(pb::LocalReadResultResponse {}))
    }

    async fn report_scc_completion(
        &self,
        request: Request<pb::SccCompletionReportRequest>,
    ) -> std::result::Result<Response<pb::SccCompletionReportResponse>, Status> {
        let request = request.into_inner();
        self.runtime
            .route_scc_completion_report(SccCompletionReport {
                batch_id: request.batch_id,
                from_shard: request.from_shard,
                failed_tx_indices: request
                    .failed_tx_indices
                    .into_iter()
                    .map(|index| index as usize)
                    .collect(),
            })
            .await
            .map_err(Status::from)?;
        Ok(Response::new(pb::SccCompletionReportResponse {}))
    }

    async fn get_tx_result(
        &self,
        request: Request<pb::GetTxResultRequest>,
    ) -> std::result::Result<Response<pb::GetTxResultResponse>, Status> {
        let tx_id = request.into_inner().tx_id;
        let result = self
            .runtime
            .get_tx_result(tx_id)
            .await
            .map_err(Status::from)?;
        let (status, tx_result) = match result {
            ClientTxResult::Ready(result) => {
                (pb::TxResultStatus::Ready as i32, tx_result_to_i32(result))
            }
            ClientTxResult::NotResponsible => (
                pb::TxResultStatus::NotResponsible as i32,
                pb::TxResult::Unspecified as i32,
            ),
        };
        Ok(Response::new(pb::GetTxResultResponse {
            tx_id,
            shard_id: self.runtime.shard_id(),
            status,
            result: tx_result,
        }))
    }

    async fn dump_state(
        &self,
        _request: Request<pb::DumpStateRequest>,
    ) -> std::result::Result<Response<pb::DumpStateResponse>, Status> {
        let state = self.runtime.dump_state().map_err(Status::from)?;
        let scc_reorders = self.runtime.dump_scc_reorders().await;
        let scheduler_profiles = self.runtime.dump_scheduler_profiles().await;
        Ok(Response::new(pb::DumpStateResponse {
            entries: inode_entries_to_proto(&state),
            scc_reorders: scc_reorder_records_to_proto(&scc_reorders),
            scheduler_profiles: scheduler_profile_records_to_proto(&scheduler_profiles),
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

    async fn submit_tx(
        &self,
        request: Request<pb::SubmitTxRequest>,
    ) -> std::result::Result<Response<pb::SubmitTxResponse>, Status> {
        let request = request.into_inner();
        let op = fs_op_from_proto(request.op).map_err(Status::from)?;
        let ack = self.runtime.submit_tx(op).await.map_err(Status::from)?;
        Ok(Response::new(pb::SubmitTxResponse {
            tx_id: ack.tx_id,
            result_shard: ack.result_shard,
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
