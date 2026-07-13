// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::planner::DefaultDistributedPlanner;
use crate::state::execution_stage::ExecutionStage;

use crate::state::execution_graph::{
    ExecutionGraphBox, RunningTaskInfo, StaticExecutionGraph, TaskDescription,
    TaskStatusUpdateResult,
};
use crate::state::executor_manager::ExecutorManager;

use ballista_core::JobStatusSubscriber;
use ballista_core::error::BallistaError;
use ballista_core::error::Result;
use ballista_core::extension::{SessionConfigExt, SessionConfigHelperExt};
use datafusion::prelude::SessionConfig;
use rand::distr::Alphanumeric;

use crate::cluster::JobState;
use crate::scheduler_server::timestamp_millis;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::{
    FailedJob, JobStatus, MultiTaskDefinition, TaskDefinition, TaskId, TaskStatus,
    job_status,
};
use ballista_core::serde::scheduler::ExecutorMetadata;
use dashmap::DashMap;

use crate::state::aqe::AdaptiveExecutionGraph;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{debug, error, info, trace, warn};
use rand::{Rng, rng};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, RwLock, watch};

type ActiveJobCache = Arc<DashMap<String, JobInfoCache>>;
type JobLifecycleControls = Arc<DashMap<String, Arc<JobLifecycleControl>>>;

struct JobLifecycleControl {
    phase: Mutex<JobLifecyclePhase>,
    publication_finished: watch::Sender<bool>,
}

impl JobLifecycleControl {
    fn new() -> Self {
        let (publication_finished, _) = watch::channel(false);
        Self {
            phase: Mutex::new(JobLifecyclePhase::Queued),
            publication_finished,
        }
    }
}

struct PublicationGuard {
    lifecycle: Arc<JobLifecycleControl>,
}

impl Drop for PublicationGuard {
    fn drop(&mut self) {
        self.lifecycle.publication_finished.send_replace(true);
    }
}

#[derive(Debug)]
enum JobLifecyclePhase {
    Queued,
    Publishing,
    Active,
    Aborted { reason: String },
}

// TODO move to configuration file
/// Default maximum number of failure attempts for task-level retry before the task is considered failed.
pub const TASK_MAX_FAILURES: usize = 4;
/// Default maximum number of failure attempts for stage-level retry before the stage is considered failed.
pub const STAGE_MAX_FAILURES: usize = 4;

/// One active job's progress at a single point in time, as captured by
/// [`TaskManager::capture_progress_snapshot`]. Compared cycle-over-cycle
/// by the scheduler's stuck-query detector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JobProgressSnapshot {
    pub job_id: String,
    pub is_terminal: bool,
    pub stages: JobProgressStages,
}

/// Per-stage progress for one job, or a marker that we could not read
/// the graph within our timeout budget (itself a diagnostic state).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JobProgressStages {
    Readable(Vec<StageProgress>),
    /// The graph's read lock could not be acquired within the snapshot
    /// budget. Most commonly this means a writer is holding the lock,
    /// but it can also happen under runtime starvation or extreme
    /// read contention.
    Unreadable,
}

/// One row of the per-stage progress snapshot.
///
/// For a `Running` stage:
/// - `partitions` is the total number of partitions the stage will produce.
/// - `assigned` is the number of partitions that have been bound to an
///   executor (whether or not that task has completed).
/// - `completed` is reported as `0`. The execution graph does not surface
///   per-partition completion within a running stage, so this field only
///   becomes meaningful once the stage transitions to `Successful`.
///
/// For a `Resolved` stage `partitions` is set and `assigned`/`completed`
/// are both `0` (no work has begun). For `Unresolved` the partition count
/// is not yet known. For `Successful` all three fields are equal. For
/// `Failed` only `partitions` is meaningful.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageProgress {
    pub stage_id: usize,
    pub variant: StageVariant,
    pub partitions: usize,
    pub assigned: usize,
    pub completed: usize,
}

/// Discriminant for an [`ExecutionStage`], used by the progress snapshot
/// and the stuck-query detector. Mirrors the variants of `ExecutionStage`
/// so that exhaustive `match`es here will fail to compile if a new stage
/// variant is added upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StageVariant {
    Unresolved,
    Resolved,
    Running,
    Successful,
    Failed,
}

fn stage_snapshot(stage_id: usize, stage: &ExecutionStage) -> StageProgress {
    let (variant, partitions, assigned, completed) = match stage {
        // UnResolved stages have no partition count yet — they are waiting
        // on upstream output to materialize their partitioning.
        ExecutionStage::UnResolved(_) => (StageVariant::Unresolved, 0, 0, 0),
        ExecutionStage::Resolved(s) => (StageVariant::Resolved, s.partitions, 0, 0),
        ExecutionStage::Running(s) => {
            let assigned = s.task_infos.iter().filter(|i| i.is_some()).count();
            (StageVariant::Running, s.partitions, assigned, 0)
        }
        ExecutionStage::Successful(s) => (
            StageVariant::Successful,
            s.partitions,
            s.partitions,
            s.partitions,
        ),
        ExecutionStage::Failed(s) => (StageVariant::Failed, s.partitions, 0, 0),
    };
    StageProgress {
        stage_id,
        variant,
        partitions,
        assigned,
        completed,
    }
}

/// Trait for launching tasks on executors.
///
/// Implementations handle the communication with executors to start task execution.
#[async_trait::async_trait]
pub trait TaskLauncher: Send + Sync + 'static {
    /// Launches the given tasks on the specified executor.
    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<MultiTaskDefinition>,
        executor_manager: &ExecutorManager,
    ) -> Result<()>;
}

struct DefaultTaskLauncher {
    scheduler_id: String,
}

impl DefaultTaskLauncher {
    pub fn new(scheduler_id: String) -> Self {
        Self { scheduler_id }
    }
}

#[async_trait::async_trait]
impl TaskLauncher for DefaultTaskLauncher {
    async fn launch_tasks(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<MultiTaskDefinition>,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        if log::max_level() >= log::Level::Info {
            let tasks_ids: Vec<String> = tasks
                .iter()
                .map(|task| {
                    let task_ids: Vec<u32> = task
                        .task_ids
                        .iter()
                        .map(|task_id| task_id.partition_id)
                        .collect();
                    format!("{}/{}/{:?}", task.job_id, task.stage_id, task_ids)
                })
                .collect();
            info!(
                "Launching multi task on executor {:?} for {:?}",
                executor.id, tasks_ids
            );
        }
        executor_manager
            .launch_multi_task(&executor.id, tasks, self.scheduler_id.clone())
            .await?;
        Ok(())
    }
}

/// Manages task scheduling and execution for the Ballista scheduler.
///
/// The `TaskManager` is responsible for:
/// - Queuing and submitting jobs
/// - Tracking job and task status
/// - Launching tasks on executors
/// - Handling task failures and retries
/// - Managing the lifecycle of execution graphs
#[derive(Clone)]
pub struct TaskManager<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    /// Persistent job state storage.
    state: Arc<dyn JobState>,
    /// Codec for serializing/deserializing logical and physical plans.
    codec: BallistaCodec<T, U>,
    /// Unique identifier for this scheduler instance.
    scheduler_id: String,
    /// Cache for active jobs curated by this scheduler.
    active_job_cache: ActiveJobCache,
    /// Per-job gate that makes cancellation linearizable with persistent-state
    /// submission and active-cache publication.
    job_lifecycles: JobLifecycleControls,
    /// Task launcher implementation.
    launcher: Arc<dyn TaskLauncher>,
}

/// Cache for active job information managed by this scheduler.
///
/// Contains the execution graph and cached data to improve performance
/// when scheduling tasks for the job.
pub struct JobInfoCache {
    /// The execution graph for this job, protected by a read-write lock.
    pub execution_graph: Arc<RwLock<ExecutionGraphBox>>,
    /// Cached job status for quick access.
    pub status: Option<job_status::Status>,
    #[cfg(not(feature = "disable-stage-plan-cache"))]
    /// Cache for encoded execution stage plans to avoid redundant serialization.
    encoded_stage_plans: HashMap<usize, Vec<u8>>,
}

impl Clone for JobInfoCache {
    fn clone(&self) -> Self {
        Self {
            execution_graph: Arc::clone(&self.execution_graph),
            status: self.status.clone(),
            // Scheduling snapshots only need status and the graph handle. Do not
            // deep-copy encoded plan bytes on every revive/poll cycle.
            #[cfg(not(feature = "disable-stage-plan-cache"))]
            encoded_stage_plans: HashMap::new(),
        }
    }
}

impl JobInfoCache {
    /// Creates a new `JobInfoCache` from an execution graph.
    pub fn new(graph: ExecutionGraphBox) -> Self {
        let status = graph.status().status.clone();

        Self {
            execution_graph: Arc::new(RwLock::new(graph)),
            status,
            #[cfg(not(feature = "disable-stage-plan-cache"))]
            encoded_stage_plans: HashMap::new(),
        }
    }
    #[cfg(not(feature = "disable-stage-plan-cache"))]
    fn encode_stage_plan<U: AsExecutionPlan>(
        &mut self,
        stage_id: usize,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Vec<u8>> {
        if let Some(plan) = self.encoded_stage_plans.get(&stage_id) {
            Ok(plan.clone())
        } else {
            let mut plan_buf: Vec<u8> = vec![];
            let plan_proto = U::try_from_physical_plan(plan.clone(), codec)?;
            plan_proto.try_encode(&mut plan_buf)?;
            self.encoded_stage_plans.insert(stage_id, plan_buf.clone());

            Ok(plan_buf)
        }
    }

    #[cfg(feature = "disable-stage-plan-cache")]
    fn encode_stage_plan<U: AsExecutionPlan>(
        &mut self,
        _stage_id: usize,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Vec<u8>> {
        let mut plan_buf: Vec<u8> = vec![];
        let plan_proto = U::try_from_physical_plan(plan.clone(), codec)?;
        plan_proto.try_encode(&mut plan_buf)?;

        Ok(plan_buf)
    }
}

/// Tracks stage state changes during task status updates.
///
/// This struct is used internally to batch stage state transitions
/// after processing task status updates.
#[derive(Clone)]
pub struct UpdatedStages {
    /// Stage IDs that have been resolved and are ready to run.
    pub resolved_stages: HashSet<usize>,
    /// Stage IDs that have completed successfully.
    pub successful_stages: HashSet<usize>,
    /// Stage IDs that have failed, mapped to their error messages.
    pub failed_stages: HashMap<usize, String>,
    /// Running stages that need to be rolled back, mapped to failure reasons.
    pub rollback_running_stages: HashMap<usize, HashSet<String>>,
    /// Successful stages that need to be re-run due to lost outputs.
    pub resubmit_successful_stages: HashSet<usize>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> TaskManager<T, U> {
    fn notify_failed_subscriber(
        subscriber: Option<&JobStatusSubscriber>,
        job_id: &str,
        job_name: &str,
        queued_at: u64,
        error: &str,
    ) {
        let Some(subscriber) = subscriber else {
            return;
        };

        let timestamp = timestamp_millis();
        let status = JobStatus {
            job_id: job_id.to_owned(),
            job_name: job_name.to_owned(),
            status: Some(job_status::Status::Failed(FailedJob {
                error: error.to_owned(),
                queued_at,
                started_at: 0,
                ended_at: timestamp,
            })),
        };

        if matches!(subscriber.try_send(status), Err(TrySendError::Full(_))) {
            error!(
                "jobs notification subscriber for job {job_id} is blocked, can't deliver status update, job notification will be missed"
            );
        }
    }

    /// Creates a new `TaskManager` with the default task launcher.
    pub fn new(
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
    ) -> Self {
        Self {
            state,
            codec,
            scheduler_id: scheduler_id.clone(),
            active_job_cache: Arc::new(DashMap::new()),
            job_lifecycles: Arc::new(DashMap::new()),
            launcher: Arc::new(DefaultTaskLauncher::new(scheduler_id)),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_launcher(
        state: Arc<dyn JobState>,
        codec: BallistaCodec<T, U>,
        scheduler_id: String,
        launcher: Arc<dyn TaskLauncher>,
    ) -> Self {
        Self {
            state,
            codec,
            scheduler_id,
            active_job_cache: Arc::new(DashMap::new()),
            job_lifecycles: Arc::new(DashMap::new()),
            launcher,
        }
    }

    fn job_lifecycle(&self, job_id: &str) -> Arc<JobLifecycleControl> {
        Arc::clone(
            self.job_lifecycles
                .entry(job_id.to_owned())
                .or_insert_with(|| Arc::new(JobLifecycleControl::new()))
                .value(),
        )
    }

    /// Enqueue a job for scheduling
    pub fn queue_job(&self, job_id: &str, job_name: &str, queued_at: u64) -> Result<()> {
        self.state.accept_job(job_id, job_name, queued_at)?;
        // Do not replace an existing control: an early cancellation for this
        // ID is a tombstone that a late JobQueued event must respect.
        self.job_lifecycle(job_id);
        Ok(())
    }

    /// Get the number of queued jobs. If it's big, then it means the scheduler is too busy.
    /// In normal case, it's better to be 0.
    pub fn pending_job_number(&self) -> usize {
        self.state.pending_job_number()
    }

    /// Get the number of running jobs.
    pub fn running_job_number(&self) -> usize {
        self.active_job_cache.len()
    }

    /// Capture one snapshot of every active job's per-stage progress for
    /// the scheduler's stuck-query detector. Uses a 500ms read-acquire
    /// budget per graph; a job whose read cannot be acquired in time is
    /// recorded as [`JobProgressStages::Unreadable`] rather than blocking
    /// the snapshot. This budget exhaustion is itself a useful diagnostic
    /// state — most often it means a writer is holding the lock, though
    /// runtime starvation or extreme read contention can produce the same
    /// outcome.
    ///
    /// Returned snapshots are diffed cycle-over-cycle by the detector; a
    /// long run of identical snapshots while executors are alive and the
    /// job is not terminal is what triggers the operator-facing warning.
    pub(crate) async fn capture_progress_snapshot(&self) -> Vec<JobProgressSnapshot> {
        let active_jobs: Vec<_> = self
            .active_job_cache
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    entry.value().status.clone(),
                    Arc::clone(&entry.value().execution_graph),
                )
            })
            .collect();

        let mut out = Vec::with_capacity(active_jobs.len());
        for (job_id, status, execution_graph) in active_jobs {
            let is_terminal = matches!(
                status,
                Some(job_status::Status::Successful(_))
                    | Some(job_status::Status::Failed(_))
            );
            let stages = match tokio::time::timeout(
                Duration::from_millis(500),
                execution_graph.read(),
            )
            .await
            {
                Ok(graph) => {
                    let mut stages = Vec::new();
                    for (stage_id, stage) in graph.stages() {
                        stages.push(stage_snapshot(*stage_id, stage));
                    }
                    stages.sort_by_key(|s| s.stage_id);
                    JobProgressStages::Readable(stages)
                }
                Err(_) => JobProgressStages::Unreadable,
            };
            out.push(JobProgressSnapshot {
                job_id,
                is_terminal,
                stages,
            });
        }
        out
    }

    /// Get the total number of pending tasks across all active jobs.
    ///
    /// A pending task is a task that is available to schedule on an executor
    /// but cannot be scheduled because no resources are available.
    ///
    /// NOTE: This method iterates over all active jobs and acquires read locks
    /// on each execution graph. It should NOT be called frequently (e.g., in a
    /// hot loop or after every event) as it can cause lock contention with
    /// concurrent task binding operations.
    pub async fn total_pending_tasks(&self) -> usize {
        let active_jobs: Vec<_> = self
            .active_job_cache
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    Arc::clone(&entry.value().execution_graph),
                )
            })
            .collect();

        let mut total = 0;
        for (job_id, execution_graph) in active_jobs {
            // Use a timeout to avoid blocking indefinitely if there's lock contention.
            // If we can't acquire the lock within the timeout, skip this job's count
            // rather than blocking the metrics collection.
            match tokio::time::timeout(Duration::from_millis(100), execution_graph.read())
                .await
            {
                Ok(graph) => {
                    total += graph.available_tasks();
                }
                Err(_) => {
                    // Lock acquisition timed out, skip this job
                    trace!(
                        "Skipping pending task count for job {} due to lock contention",
                        job_id
                    );
                }
            }
        }
        total
    }

    /// Generate an ExecutionGraph for the job and save it to the persistent state.
    /// By default, this job will be curated by the scheduler which receives it.
    /// Then we will also save it to the active execution graph
    #[allow(clippy::too_many_arguments)]
    pub async fn submit_job(
        &self,
        job_id: &str,
        job_name: &str,
        session_id: &str,
        plan: Arc<dyn ExecutionPlan>,
        queued_at: u64,
        session_config: Arc<SessionConfig>,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        let lifecycle = self.job_lifecycle(job_id);
        let aborted_before_planning = {
            let phase = lifecycle.phase.lock().await;
            if let JobLifecyclePhase::Aborted { reason } = &*phase {
                Some(reason.clone())
            } else {
                None
            }
        };
        if let Some(reason) = aborted_before_planning {
            if let Err(e) = self
                .state
                .fail_unscheduled_job(job_id, reason.clone())
                .await
            {
                debug!(
                    "Job {job_id} was already removed from the queued state after cancellation: {e}"
                );
            }
            Self::notify_failed_subscriber(
                subscriber.as_ref(),
                job_id,
                job_name,
                queued_at,
                &reason,
            );
            return Err(BallistaError::Cancelled);
        }

        let mut planner = DefaultDistributedPlanner::new();

        let mut graph = if session_config.ballista_adaptive_query_planner_enabled() {
            debug!("Using adaptive query planner (AQE) for job planning");
            warn!(
                "Adaptive Query Planning is EXPERIMENTAL, should be used for testing purposes only!"
            );
            Box::new(AdaptiveExecutionGraph::try_new(
                &self.scheduler_id,
                job_id,
                job_name,
                session_id,
                plan,
                queued_at,
                session_config,
            )?) as ExecutionGraphBox
        } else {
            debug!("Using static query planner for job planning");
            Box::new(StaticExecutionGraph::new(
                &self.scheduler_id,
                job_id,
                job_name,
                session_id,
                plan,
                queued_at,
                session_config,
                &mut planner,
            )?) as ExecutionGraphBox
        };

        info!("Submitting execution graph:\n\n{graph:?}");

        {
            let mut phase = lifecycle.phase.lock().await;
            match &*phase {
                JobLifecyclePhase::Queued => {
                    lifecycle.publication_finished.send_replace(false);
                    *phase = JobLifecyclePhase::Publishing;
                }
                JobLifecyclePhase::Aborted { reason } => {
                    let reason = reason.clone();
                    drop(phase);
                    if let Err(e) = self
                        .state
                        .fail_unscheduled_job(job_id, reason.clone())
                        .await
                    {
                        debug!(
                            "Job {job_id} was already removed from the queued state after cancellation: {e}"
                        );
                    }
                    Self::notify_failed_subscriber(
                        subscriber.as_ref(),
                        job_id,
                        job_name,
                        queued_at,
                        &reason,
                    );
                    return Err(BallistaError::Cancelled);
                }
                JobLifecyclePhase::Publishing | JobLifecyclePhase::Active => {
                    return Err(BallistaError::Internal(format!(
                        "Job {job_id} is already being submitted"
                    )));
                }
            }
        }
        let _publication_guard = PublicationGuard {
            lifecycle: Arc::clone(&lifecycle),
        };

        let submit_result = self
            .state
            .submit_job(job_id.to_string(), &graph, subscriber.clone())
            .await;

        if let Err(e) = submit_result {
            let mut phase = lifecycle.phase.lock().await;
            if let JobLifecyclePhase::Aborted { reason } = &*phase {
                let reason = reason.clone();
                drop(phase);
                Self::notify_failed_subscriber(
                    subscriber.as_ref(),
                    job_id,
                    job_name,
                    queued_at,
                    &reason,
                );
                return Err(BallistaError::Cancelled);
            }
            if matches!(&*phase, JobLifecyclePhase::Publishing) {
                *phase = JobLifecyclePhase::Queued;
            }
            return Err(e);
        }

        let mut phase = lifecycle.phase.lock().await;
        if let JobLifecyclePhase::Aborted { reason } = &*phase {
            let reason = reason.clone();
            drop(phase);
            graph.fail_job(reason);
            self.state.save_job(job_id, &graph).await?;
            return Err(BallistaError::Cancelled);
        }
        if !matches!(&*phase, JobLifecyclePhase::Publishing) {
            return Err(BallistaError::Internal(format!(
                "Job {job_id} reached an invalid lifecycle phase before publication"
            )));
        }

        // The phase lock is the publication linearization point. Cancellation
        // either records its tombstone before this block (so publication is
        // refused above) or acquires the same lock afterwards and removes the
        // graph before returning.
        graph.revive();
        self.active_job_cache
            .insert(job_id.to_owned(), JobInfoCache::new(graph));
        *phase = JobLifecyclePhase::Active;

        Ok(())
    }

    /// Returns a snapshot of currently running jobs from the cache.
    pub fn get_running_job_cache(&self) -> Arc<HashMap<String, JobInfoCache>> {
        let ret = self
            .active_job_cache
            .iter()
            .filter_map(|pair| {
                let (job_id, job_info) = pair.pair();
                if matches!(job_info.status, Some(job_status::Status::Running(_))) {
                    Some((job_id.clone(), job_info.clone()))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        Arc::new(ret)
    }

    /// Get a list of active job ids
    pub async fn get_jobs(&self) -> Result<Vec<JobOverview>> {
        let job_ids = self.state.get_jobs().await?;

        let mut jobs = vec![];
        for job_id in &job_ids {
            if let Some(cached) = self.get_active_execution_graph(job_id) {
                let graph = cached.read().await;
                jobs.push(graph.deref().into());
            } else {
                let graph = self.state
                    .get_execution_graph(job_id)
                    .await?
                    .ok_or_else(|| BallistaError::Internal(format!("Error getting job overview, no execution graph found for job {job_id}")))?;
                jobs.push((&graph).into());
            }
        }
        Ok(jobs)
    }

    /// Get the status of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs, and then in Failed jobs
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            let guard = graph.read().await;

            Ok(Some(guard.status().clone()))
        } else {
            self.state.get_job_status(job_id).await
        }
    }

    /// Get the execution graph of of a job. First look in the active cache.
    /// If no one found, then in the Active/Completed jobs.
    ///
    /// Exposed as `pub` so embedded callers (e.g., Spice's distributed
    /// task_history writer) can walk per-stage and per-task state directly
    /// without going through a gRPC method.
    pub async fn get_job_execution_graph(
        &self,
        job_id: &str,
    ) -> Result<Option<ExecutionGraphBox>> {
        if let Some(cached) = self.get_active_execution_graph(job_id) {
            let guard = cached.read().await;

            Ok(Some(guard.deref().cloned()))
        } else {
            let graph = self.state.get_execution_graph(job_id).await?;

            Ok(graph)
        }
    }

    /// Update given task statuses in the respective job and return a `TaskStatusUpdateResult`
    /// containing:
    /// 1. A list of `QueryStageSchedulerEvent` to publish.
    /// 2. Metrics information about stage/task lifecycle changes.
    pub(crate) async fn update_task_statuses(
        &self,
        executor: &ExecutorMetadata,
        task_status: Vec<TaskStatus>,
    ) -> Result<TaskStatusUpdateResult> {
        let mut job_updates: HashMap<String, Vec<TaskStatus>> = HashMap::new();
        for status in task_status {
            trace!("Task Update\n{status:?}");
            let job_id = status.job_id.clone();
            let job_task_statuses = job_updates.entry(job_id).or_default();
            job_task_statuses.push(status);
        }

        let mut combined_result = TaskStatusUpdateResult::default();
        for (job_id, statuses) in job_updates {
            let num_tasks = statuses.len();
            debug!("Updating {num_tasks} tasks in job {job_id}");

            let events = if let Some(cached) = self.get_active_execution_graph(&job_id) {
                let mut graph = cached.write().await;
                graph.update_task_status(
                    executor,
                    statuses,
                    TASK_MAX_FAILURES,
                    STAGE_MAX_FAILURES,
                )?
            } else {
                // TODO Deal with curator changed case
                error!(
                    "Fail to find job {job_id} in the active cache and it may not be curated by this scheduler"
                );
                vec![]
            };

            // Combine events from all jobs
            combined_result.events.extend(events);
            // Note: metrics are not available through the trait interface.
            // Use update_task_status_with_metrics on StaticExecutionGraph for metrics tracking.
        }

        Ok(combined_result)
    }

    /// Mark a job to success. This will create a key under the CompletedJobs keyspace
    /// and remove the job from ActiveJobs
    pub(crate) async fn succeed_job(&self, job_id: &str) -> Result<()> {
        debug!("Moving job {job_id} from Active to Success");

        if let Some(graph) = self.remove_active_execution_graph(job_id) {
            let graph = graph.read().await;
            if graph.is_successful() {
                self.state.save_job(job_id, &graph).await?;
            } else {
                error!("Job {job_id} has not finished and cannot be completed");
                return Ok(());
            }
        } else {
            warn!("Fail to find job {job_id} in the cache");
        }

        Ok(())
    }

    /// Cancel the job and return a Vec of running tasks need to cancel
    pub(crate) async fn cancel_job(
        &self,
        job_id: &str,
    ) -> Result<(Vec<RunningTaskInfo>, usize)> {
        self.abort_job(job_id, "Cancelled".to_owned()).await
    }

    /// Abort the job and return a Vec of running tasks need to cancel
    pub(crate) async fn abort_job(
        &self,
        job_id: &str,
        failure_reason: String,
    ) -> Result<(Vec<RunningTaskInfo>, usize)> {
        let lifecycle = self.job_lifecycle(job_id);
        let mut publication_finished = lifecycle.publication_finished.subscribe();
        let (graph, was_publishing) = {
            let mut phase = lifecycle.phase.lock().await;
            let was_publishing = matches!(&*phase, JobLifecyclePhase::Publishing);
            if !matches!(&*phase, JobLifecyclePhase::Aborted { .. }) {
                *phase = JobLifecyclePhase::Aborted {
                    reason: failure_reason.clone(),
                };
            }
            // Removing the graph under the same gate used for publication
            // prevents a late submit from inserting it after cancellation.
            (self.remove_active_execution_graph(job_id), was_publishing)
        };

        let (tasks_to_cancel, pending_tasks) = if let Some(graph) = graph {
            let mut guard = graph.write().await;

            let pending_tasks = guard.available_tasks();
            let running_tasks = guard.running_tasks();

            info!(
                "Cancelling {} running tasks for job {}",
                running_tasks.len(),
                job_id
            );

            guard.fail_job(failure_reason);

            self.state.save_job(job_id, &guard).await?;

            (running_tasks, pending_tasks)
        } else {
            // Cancellation may arrive while the job is queued or while
            // `state.submit_job` is in flight. Removing a queued job makes a
            // late submit fail; if persistence already moved it to Running,
            // the publisher observes the tombstone and saves its graph failed
            // instead of exposing it through the active cache.
            let fail_unscheduled_result = self
                .state
                .fail_unscheduled_job(job_id, failure_reason)
                .await;
            if let Err(e) = &fail_unscheduled_result {
                debug!(
                    "Job {job_id} was not in queued state while cancellation raced with publication: {e}"
                );
            }

            if was_publishing && fail_unscheduled_result.is_err() {
                // Persistent submission already won the queued-state race.
                // Wait for the publisher to observe the tombstone, persist its
                // graph as failed, and decline active-cache publication before
                // acknowledging cancellation.
                while !*publication_finished.borrow_and_update() {
                    publication_finished.changed().await.map_err(|_| {
                        BallistaError::Internal(format!(
                            "Job {job_id} publication ended without reporting completion"
                        ))
                    })?;
                }

                let status = self.state.get_job_status(job_id).await?;
                if !matches!(
                    status.as_ref().and_then(|status| status.status.as_ref()),
                    Some(job_status::Status::Failed(_))
                ) {
                    return Err(BallistaError::Internal(format!(
                        "Job {job_id} cancellation did not reach a durable failed state"
                    )));
                }
            }
            (vec![], 0)
        };

        Ok((tasks_to_cancel, pending_tasks))
    }

    /// Mark a unscheduled job as failed. This will create a key under the FailedJobs keyspace
    /// and remove the job from ActiveJobs or QueuedJobs
    pub async fn fail_unscheduled_job(
        &self,
        job_id: &str,
        failure_reason: String,
    ) -> Result<()> {
        let lifecycle = self.job_lifecycle(job_id);
        {
            let mut phase = lifecycle.phase.lock().await;
            if !matches!(&*phase, JobLifecyclePhase::Aborted { .. }) {
                *phase = JobLifecyclePhase::Aborted {
                    reason: failure_reason.clone(),
                };
            }
        }
        let result = self
            .state
            .fail_unscheduled_job(job_id, failure_reason)
            .await;

        if result.is_ok() {
            // A planning failure is terminal and its publisher has already
            // returned. Remove only the lifecycle instance that we failed so
            // an unlikely job-ID reuse cannot lose its newer control.
            self.job_lifecycles
                .remove_if(job_id, |_, current| Arc::ptr_eq(current, &lifecycle));
        }

        result
    }

    /// Updates the job state and returns the number of new available tasks.
    pub async fn update_job(&self, job_id: &str) -> Result<usize> {
        debug!("Update active job {job_id}");
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            let mut graph = graph.write().await;

            let curr_available_tasks = graph.available_tasks();

            graph.revive();

            info!("Saving job with status {:?}", graph.status());

            self.state.save_job(job_id, &graph).await?;

            let new_tasks = graph.available_tasks() - curr_available_tasks;

            Ok(new_tasks)
        } else {
            warn!("Fail to find job {job_id} in the cache");

            Ok(0)
        }
    }

    /// Handles executor loss by resetting affected tasks and stages.
    ///
    /// Returns a list of running tasks that need to be cancelled.
    pub async fn executor_lost(&self, executor_id: &str) -> Result<Vec<RunningTaskInfo>> {
        // Collect all the running task need to cancel when there are running stages rolled back.
        let mut running_tasks_to_cancel: Vec<RunningTaskInfo> = vec![];
        let active_graphs: Vec<_> = self
            .active_job_cache
            .iter()
            .map(|entry| Arc::clone(&entry.value().execution_graph))
            .collect();

        {
            for execution_graph in active_graphs {
                let mut graph = execution_graph.write().await;
                let reset = graph.reset_stages_on_lost_executor(executor_id)?;
                if !reset.0.is_empty() {
                    running_tasks_to_cancel.extend(reset.1);
                }
            }
        }

        Ok(running_tasks_to_cancel)
    }

    /// Retrieves the number of available tasks for the given job.
    ///
    /// The value returned is a point-in-time snapshot and may change immediately.
    pub async fn get_available_task_count(&self, job_id: &str) -> Result<usize> {
        if let Some(graph) = self.get_active_execution_graph(job_id) {
            let available_tasks = graph.read().await.available_tasks();
            Ok(available_tasks)
        } else {
            warn!("Fail to find job {job_id} in the cache");
            Ok(0)
        }
    }

    /// Prepares a task definition for a single task to be sent to an executor.
    #[allow(dead_code)]
    pub fn prepare_task_definition(
        &self,
        task: TaskDescription,
    ) -> Result<TaskDefinition> {
        debug!("Preparing task definition for {task:?}");

        let job_id = task.partition.job_id.clone();
        let stage_id = task.partition.stage_id;

        if let Some(mut job_info) = self.active_job_cache.get_mut(&job_id) {
            let plan = job_info.encode_stage_plan::<PhysicalPlanNode>(
                stage_id,
                &task.plan,
                self.codec.physical_extension_codec(),
            )?;

            let task_definition = TaskDefinition {
                task_id: task.task_id as u32,
                task_attempt_num: task.task_attempt as u32,
                job_id,
                stage_id: stage_id as u32,
                stage_attempt_num: task.stage_attempt_num as u32,
                partition_id: task.partition.partition_id as u32,
                plan,
                session_id: task.session_id,
                launch_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                props: task.session_config.to_key_value_pairs(),
            };
            Ok(task_definition)
        } else {
            Err(BallistaError::General(format!(
                "Cannot prepare task definition for job {job_id} which is not in active cache"
            )))
        }
    }

    /// Launch the given tasks on the specified executor
    pub(crate) async fn launch_multi_task(
        &self,
        executor: &ExecutorMetadata,
        tasks: Vec<Vec<TaskDescription>>,
        executor_manager: &ExecutorManager,
    ) -> Result<()> {
        let mut multi_tasks = vec![];
        for stage_tasks in tasks {
            match self.prepare_multi_task_definition(stage_tasks) {
                Ok(stage_tasks) => multi_tasks.extend(stage_tasks),
                Err(e) => error!("Fail to prepare task definition: {e:?}"),
            }
        }

        if !multi_tasks.is_empty() {
            self.launcher
                .launch_tasks(executor, multi_tasks, executor_manager)
                .await
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    /// Prepare a MultiTaskDefinition with multiple tasks belonging to the same job stage
    fn prepare_multi_task_definition(
        &self,
        tasks: Vec<TaskDescription>,
    ) -> Result<Vec<MultiTaskDefinition>> {
        if let Some(task) = tasks.first() {
            let session_id = task.session_id.clone();
            let job_id = task.partition.job_id.clone();
            let stage_id = task.partition.stage_id;
            let stage_attempt_num = task.stage_attempt_num;

            if log::max_level() >= log::Level::Debug {
                let task_ids: Vec<usize> = tasks
                    .iter()
                    .map(|task| task.partition.partition_id)
                    .collect();
                debug!(
                    "Preparing multi task definition for tasks {task_ids:?} belonging to job stage {job_id}/{stage_id}"
                );
                trace!("With task details {tasks:?}");
            }

            if let Some(mut job_info) = self.active_job_cache.get_mut(&job_id) {
                let plan = job_info.encode_stage_plan::<PhysicalPlanNode>(
                    stage_id,
                    &task.plan,
                    self.codec.physical_extension_codec(),
                )?;

                let launch_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let mut multi_tasks = vec![];
                let props = task.session_config.to_key_value_pairs();
                let task_ids = tasks
                    .into_iter()
                    .map(|task| TaskId {
                        task_id: task.task_id as u32,
                        task_attempt_num: task.task_attempt as u32,
                        partition_id: task.partition.partition_id as u32,
                    })
                    .collect();
                multi_tasks.push(MultiTaskDefinition {
                    task_ids,
                    job_id,
                    stage_id: stage_id as u32,
                    stage_attempt_num: stage_attempt_num as u32,
                    plan,
                    session_id,
                    launch_time,
                    props,
                });

                Ok(multi_tasks)
            } else {
                Err(BallistaError::General(format!(
                    "Cannot prepare multi task definition for job {job_id} which is not in active cache"
                )))
            }
        } else {
            Err(BallistaError::General(
                "Cannot prepare multi task definition for an empty vec".to_string(),
            ))
        }
    }

    /// Get the `ExecutionGraph` for the given job ID from cache
    pub(crate) fn get_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraphBox>>> {
        self.active_job_cache
            .get(job_id)
            .as_deref()
            .map(|cached| cached.execution_graph.clone())
    }

    /// Remove the `ExecutionGraph` for the given job ID from cache
    pub(crate) fn remove_active_execution_graph(
        &self,
        job_id: &str,
    ) -> Option<Arc<RwLock<ExecutionGraphBox>>> {
        self.active_job_cache
            .remove(job_id)
            .map(|value| value.1.execution_graph)
    }

    /// Generates a new random 7-character alphanumeric job ID.
    pub fn generate_job_id(&self) -> String {
        let mut rng = rng();
        std::iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(7)
            .collect()
    }

    /// Clean up a failed job in FailedJobs Keyspace by delayed clean_up_interval seconds
    pub(crate) fn clean_up_job_delayed(&self, job_id: String, clean_up_interval: u64) {
        if clean_up_interval == 0 {
            info!(
                "The interval is 0 and the clean up for the failed job state {job_id} will not triggered"
            );
            return;
        }

        let state = self.state.clone();
        let job_lifecycles = Arc::clone(&self.job_lifecycles);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(clean_up_interval)).await;
            if let Err(err) = state.remove_job(&job_id).await {
                error!("Failed to delete job {job_id}: {err:?}");
            } else {
                job_lifecycles.remove(&job_id);
            }
        });
    }
}

/// Summary information about a job for display purposes.
pub struct JobOverview {
    /// Unique identifier for this job.
    pub job_id: String,
    /// Human-readable name for this job.
    pub job_name: String,
    /// Current status of the job.
    pub status: JobStatus,
    /// Timestamp when the job started.
    pub start_time: u64,
    /// Timestamp when the job ended (0 if still running).
    pub end_time: u64,
    /// Total number of stages in the job.
    pub num_stages: usize,
    /// Number of stages that have completed successfully.
    pub completed_stages: usize,
}

impl From<&ExecutionGraphBox> for JobOverview {
    fn from(value: &ExecutionGraphBox) -> Self {
        let completed_stages = value.completed_stages();

        Self {
            job_id: value.job_id().to_string(),
            job_name: value.job_name().to_string(),
            status: value.status().clone(),
            start_time: value.start_time(),
            end_time: value.end_time(),
            num_stages: value.stage_count(),
            completed_stages,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{BallistaCluster, JobStateEventStream};
    use crate::config::SchedulerConfig;
    use ballista_core::serde::BallistaCodec;
    use ballista_core::serde::protobuf::job_status::Status;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
    use tokio::sync::Barrier;

    #[derive(Clone, Copy)]
    enum SubmitBarrierPoint {
        BeforePersistentSubmit,
        AfterPersistentSubmit,
    }

    struct BarrierJobState {
        inner: Arc<dyn JobState>,
        point: SubmitBarrierPoint,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    #[async_trait::async_trait]
    impl JobState for BarrierJobState {
        fn accept_job(&self, job_id: &str, job_name: &str, queued_at: u64) -> Result<()> {
            self.inner.accept_job(job_id, job_name, queued_at)
        }

        fn pending_job_number(&self) -> usize {
            self.inner.pending_job_number()
        }

        async fn submit_job(
            &self,
            job_id: String,
            graph: &ExecutionGraphBox,
            subscriber: Option<JobStatusSubscriber>,
        ) -> Result<()> {
            if matches!(self.point, SubmitBarrierPoint::BeforePersistentSubmit) {
                self.entered.wait().await;
                self.release.wait().await;
            }

            let result = self.inner.submit_job(job_id, graph, subscriber).await;

            if matches!(self.point, SubmitBarrierPoint::AfterPersistentSubmit) {
                self.entered.wait().await;
                self.release.wait().await;
            }

            result
        }

        async fn get_jobs(&self) -> Result<HashSet<String>> {
            self.inner.get_jobs().await
        }

        async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
            self.inner.get_job_status(job_id).await
        }

        async fn get_execution_graph(
            &self,
            job_id: &str,
        ) -> Result<Option<ExecutionGraphBox>> {
            self.inner.get_execution_graph(job_id).await
        }

        async fn save_job(&self, job_id: &str, graph: &ExecutionGraphBox) -> Result<()> {
            self.inner.save_job(job_id, graph).await
        }

        async fn fail_unscheduled_job(&self, job_id: &str, reason: String) -> Result<()> {
            self.inner.fail_unscheduled_job(job_id, reason).await
        }

        async fn remove_job(&self, job_id: &str) -> Result<()> {
            self.inner.remove_job(job_id).await
        }

        async fn try_acquire_job(
            &self,
            job_id: &str,
        ) -> Result<Option<ExecutionGraphBox>> {
            self.inner.try_acquire_job(job_id).await
        }

        async fn job_state_events(&self) -> Result<JobStateEventStream> {
            self.inner.job_state_events().await
        }

        async fn create_or_update_session(
            &self,
            session_id: &str,
            config: &SessionConfig,
        ) -> Result<Arc<SessionContext>> {
            self.inner
                .create_or_update_session(session_id, config)
                .await
        }

        async fn remove_session(&self, session_id: &str) -> Result<()> {
            self.inner.remove_session(session_id).await
        }

        fn produce_config(&self) -> SessionConfig {
            self.inner.produce_config()
        }
    }

    type TestTaskManager = TaskManager<LogicalPlanNode, PhysicalPlanNode>;

    async fn memory_job_state() -> Arc<dyn JobState> {
        BallistaCluster::new_from_config(&SchedulerConfig::default())
            .await
            .expect("to create in-memory scheduler state")
            .job_state()
    }

    fn test_task_manager(state: Arc<dyn JobState>) -> TestTaskManager {
        TaskManager::new(state, BallistaCodec::default(), "test-scheduler".to_owned())
    }

    fn test_plan() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::empty())).with_partitions(1))
    }

    async fn submit_test_job(manager: &TestTaskManager, job_id: &str) -> Result<()> {
        submit_test_job_with_subscriber(manager, job_id, None).await
    }

    async fn submit_test_job_with_subscriber(
        manager: &TestTaskManager,
        job_id: &str,
        subscriber: Option<JobStatusSubscriber>,
    ) -> Result<()> {
        manager
            .submit_job(
                job_id,
                "test-job",
                "test-session",
                test_plan(),
                1,
                Arc::new(SessionConfig::new_with_ballista()),
                subscriber,
            )
            .await
    }

    async fn assert_failed_without_active_graph(manager: &TestTaskManager, job_id: &str) {
        assert!(
            manager.get_active_execution_graph(job_id).is_none(),
            "cancelled job must not remain in the active cache"
        );
        assert_eq!(
            manager.running_job_number(),
            0,
            "cancelled job must not expose runnable tasks"
        );

        let status = manager
            .get_job_status(job_id)
            .await
            .expect("to read cancelled job status")
            .expect("cancelled job status to exist");
        assert!(
            matches!(status.status, Some(Status::Failed(ref failed)) if failed.error == "Cancelled"),
            "cancelled job should be durably failed, got {status:?}"
        );
    }

    async fn wait_for_abort_tombstone(manager: &TestTaskManager, job_id: &str) {
        loop {
            let lifecycle = manager.job_lifecycle(job_id);
            let phase = lifecycle.phase.lock().await;
            if matches!(&*phase, JobLifecyclePhase::Aborted { .. }) {
                return;
            }
            drop(phase);
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn cancellation_before_submit_is_a_tombstone() {
        let manager = test_task_manager(memory_job_state().await);
        let job_id = "cancel-before-submit";
        manager
            .queue_job(job_id, "test-job", 1)
            .expect("to queue test job");

        manager
            .cancel_job(job_id)
            .await
            .expect("to cancel queued job");
        let submit_result = submit_test_job(&manager, job_id).await;

        assert!(matches!(submit_result, Err(BallistaError::Cancelled)));
        assert_failed_without_active_graph(&manager, job_id).await;
    }

    #[tokio::test]
    async fn queued_cancellation_notifies_job_status_subscriber() {
        let manager = test_task_manager(memory_job_state().await);
        let job_id = "cancelled-subscriber";
        manager
            .queue_job(job_id, "test-job", 1)
            .expect("to queue test job");
        manager
            .cancel_job(job_id)
            .await
            .expect("to cancel queued job");

        let (subscriber, mut statuses) = tokio::sync::mpsc::channel(1);
        let submit_result =
            submit_test_job_with_subscriber(&manager, job_id, Some(subscriber)).await;

        assert!(matches!(submit_result, Err(BallistaError::Cancelled)));
        let status = statuses
            .try_recv()
            .expect("cancelled subscriber to receive a terminal status");
        assert!(
            matches!(status.status, Some(Status::Failed(ref failed)) if failed.error == "Cancelled"),
            "cancelled subscriber should receive Failed(Cancelled), got {status:?}"
        );
    }

    #[tokio::test]
    async fn planning_failure_releases_lifecycle_control() {
        let manager = test_task_manager(memory_job_state().await);
        let job_id = "planning-failure-cleanup";
        manager
            .queue_job(job_id, "test-job", 1)
            .expect("to queue test job");
        assert!(manager.job_lifecycles.contains_key(job_id));

        manager
            .fail_unscheduled_job(job_id, "planning failed".to_owned())
            .await
            .expect("to persist planning failure");

        assert!(
            !manager.job_lifecycles.contains_key(job_id),
            "completed planning failure must not retain a lifecycle control"
        );
    }

    #[tokio::test]
    async fn cancellation_while_persistent_submit_is_blocked_prevents_publication() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let state: Arc<dyn JobState> = Arc::new(BarrierJobState {
            inner: memory_job_state().await,
            point: SubmitBarrierPoint::BeforePersistentSubmit,
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let manager = test_task_manager(state);
        let job_id = "cancel-before-state-submit";
        manager
            .queue_job(job_id, "test-job", 1)
            .expect("to queue test job");

        let (subscriber, mut statuses) = tokio::sync::mpsc::channel(1);
        let submit_manager = manager.clone();
        let submit = tokio::spawn(async move {
            submit_test_job_with_subscriber(
                &submit_manager,
                "cancel-before-state-submit",
                Some(subscriber),
            )
            .await
        });
        entered.wait().await;

        manager
            .cancel_job(job_id)
            .await
            .expect("to cancel job blocked before persistent submit");
        release.wait().await;

        let submit_result = submit.await.expect("submit task not to panic");
        assert!(matches!(submit_result, Err(BallistaError::Cancelled)));
        let status = statuses
            .try_recv()
            .expect("cancelled publisher subscriber to receive a terminal status");
        assert!(
            matches!(status.status, Some(Status::Failed(ref failed)) if failed.error == "Cancelled"),
            "cancelled publisher subscriber should receive Failed(Cancelled), got {status:?}"
        );
        assert_failed_without_active_graph(&manager, job_id).await;
    }

    #[tokio::test]
    async fn cancellation_after_persistent_submit_prevents_cache_publication() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let state: Arc<dyn JobState> = Arc::new(BarrierJobState {
            inner: memory_job_state().await,
            point: SubmitBarrierPoint::AfterPersistentSubmit,
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let manager = test_task_manager(state);
        let job_id = "cancel-after-state-submit";
        manager
            .queue_job(job_id, "test-job", 1)
            .expect("to queue test job");

        let submit_manager = manager.clone();
        let submit = tokio::spawn(async move {
            submit_test_job(&submit_manager, "cancel-after-state-submit").await
        });
        entered.wait().await;

        let cancel_manager = manager.clone();
        let cancel = tokio::spawn(async move {
            cancel_manager.cancel_job("cancel-after-state-submit").await
        });
        wait_for_abort_tombstone(&manager, job_id).await;
        assert!(
            !cancel.is_finished(),
            "cancellation must wait until the publisher durably fails the job"
        );
        release.wait().await;

        cancel
            .await
            .expect("cancel task not to panic")
            .expect("to cancel job after persistent submit");
        let submit_result = submit.await.expect("submit task not to panic");
        assert!(matches!(submit_result, Err(BallistaError::Cancelled)));
        assert_failed_without_active_graph(&manager, job_id).await;
    }

    #[tokio::test]
    async fn cancellation_after_cache_publication_removes_graph() {
        let manager = test_task_manager(memory_job_state().await);
        let job_id = "cancel-after-cache-publish";
        manager
            .queue_job(job_id, "test-job", 1)
            .expect("to queue test job");
        submit_test_job(&manager, job_id)
            .await
            .expect("to publish active graph");
        assert!(manager.get_active_execution_graph(job_id).is_some());

        manager
            .cancel_job(job_id)
            .await
            .expect("to cancel active job");

        assert_failed_without_active_graph(&manager, job_id).await;
    }
}
