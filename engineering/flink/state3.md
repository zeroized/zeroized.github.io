# State(3): Checkpointing(上)
2020/11/15

注：源代码为Flink1.11.0版本

## 相关概念

## 开始Checkpointing

系统的Checkpointing由JobManager中的```CheckpointCoordinator#triggerCheckpoint(CheckpointProperties, String, boolean, boolean)```方法启动。如果系统设置了```env.enableCheckpointing(long interval)```，```triggerCheckpoint```方法会由定时任务根据设置的间隔触发```triggerCheckpoint(boolean)```；同时用户可以通过手动启动一个savepoint来触发checkpointing（```triggerSavepointInternal```方法）。

```java
// CheckpointCoordinator.class第483行
public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(boolean isPeriodic) {
	return triggerCheckpoint(checkpointProperties, null, isPeriodic, false);
}

@VisibleForTesting
public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
		CheckpointProperties props,
		@Nullable String externalSavepointLocation,
		boolean isPeriodic,
		boolean advanceToEndOfTime) {

	if (advanceToEndOfTime && !(props.isSynchronous() && props.isSavepoint())) {
		return FutureUtils.completedExceptionally(new IllegalArgumentException(
			"Only synchronous savepoints are allowed to advance the watermark to MAX."));
	}

	CheckpointTriggerRequest request = new CheckpointTriggerRequest(props, externalSavepointLocation, isPeriodic, advanceToEndOfTime);
	requestDecider
		.chooseRequestToExecute(request, isTriggering, lastCheckpointCompletionRelativeTime)
		.ifPresent(this::startTriggeringCheckpoint);
	return request.onCompletionPromise;
}
```

Checkpointing请求首先会由```CheckpointRequestDecider#chooseRequestToExecute```决定具体执行等待队列中的哪一个checkpointing请求。Decider总是会从等待队列中选择一个优先级最高的请求（savepoint对应的请求优先级总是高于定时checkpointing任务发出的请求，因此如果等待队列的最低优先级元素是非定时任务请求，意味着等待队列中所有的请求都是savepoint请求，新的请求就会被丢弃并且返回空值）。

<details>
<summary>CheckpointRequestDecider</summary>

```java
// CheckpointRequestDecider.class第96行
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator.CheckpointTriggerRequest;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.identityHashCode;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.TOO_MANY_CHECKPOINT_REQUESTS;

@SuppressWarnings("ConstantConditions")
class CheckpointRequestDecider {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointRequestDecider.class);
	private static final int LOG_TIME_IN_QUEUE_THRESHOLD_MS = 100;
	private static final int DEFAULT_MAX_QUEUED_REQUESTS = 1000;

	private final int maxConcurrentCheckpointAttempts;
	private final Consumer<Long> rescheduleTrigger;
	private final Clock clock;
	private final long minPauseBetweenCheckpoints;
	private final Supplier<Integer> pendingCheckpointsSizeSupplier;
	private final Object lock;
	@GuardedBy("lock")
	private final NavigableSet<CheckpointTriggerRequest> queuedRequests = new TreeSet<>(checkpointTriggerRequestsComparator());
	private final int maxQueuedRequests;

	CheckpointRequestDecider(
			int maxConcurrentCheckpointAttempts,
			Consumer<Long> rescheduleTrigger,
			Clock clock,
			long minPauseBetweenCheckpoints,
			Supplier<Integer> pendingCheckpointsSizeSupplier,
			Object lock) {
		this(
			maxConcurrentCheckpointAttempts,
			rescheduleTrigger,
			clock,
			minPauseBetweenCheckpoints,
			pendingCheckpointsSizeSupplier,
			lock,
			DEFAULT_MAX_QUEUED_REQUESTS
		);
	}

	CheckpointRequestDecider(
			int maxConcurrentCheckpointAttempts,
			Consumer<Long> rescheduleTrigger,
			Clock clock,
			long minPauseBetweenCheckpoints,
			Supplier<Integer> pendingCheckpointsSizeSupplier,
			Object lock,
			int maxQueuedRequests) {
		Preconditions.checkArgument(maxConcurrentCheckpointAttempts > 0);
		Preconditions.checkArgument(maxQueuedRequests > 0);
		this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
		this.rescheduleTrigger = rescheduleTrigger;
		this.clock = clock;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.pendingCheckpointsSizeSupplier = pendingCheckpointsSizeSupplier;
		this.lock = lock;
		this.maxQueuedRequests = maxQueuedRequests;
	}

	Optional<CheckpointTriggerRequest> chooseRequestToExecute(CheckpointTriggerRequest newRequest, boolean isTriggering, long lastCompletionMs) {
		synchronized (lock) {
			if (queuedRequests.size() >= maxQueuedRequests && !queuedRequests.last().isPeriodic) {
				// there are only non-periodic (ie user-submitted) requests enqueued - retain them and drop the new one
				newRequest.completeExceptionally(new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
				return Optional.empty();
			} else {
				queuedRequests.add(newRequest);
				if (queuedRequests.size() > maxQueuedRequests) {
					queuedRequests.pollLast().completeExceptionally(new CheckpointException(TOO_MANY_CHECKPOINT_REQUESTS));
				}
				Optional<CheckpointTriggerRequest> request = chooseRequestToExecute(isTriggering, lastCompletionMs);
				request.ifPresent(CheckpointRequestDecider::logInQueueTime);
				return request;
			}
		}
	}

	Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute(boolean isTriggering, long lastCompletionMs) {
		synchronized (lock) {
			Optional<CheckpointTriggerRequest> request = chooseRequestToExecute(isTriggering, lastCompletionMs);
			request.ifPresent(CheckpointRequestDecider::logInQueueTime);
			return request;
		}
	}

	/**
	 * Choose the next {@link CheckpointTriggerRequest request} to execute based on the provided candidate and the
	 * current state. Acquires a lock and may update the state.
	 * @return request to execute, if any.
	 */
	private Optional<CheckpointTriggerRequest> chooseRequestToExecute(boolean isTriggering, long lastCompletionMs) {
		Preconditions.checkState(Thread.holdsLock(lock));
		if (isTriggering || queuedRequests.isEmpty()) {
			return Optional.empty();
		}

		if (pendingCheckpointsSizeSupplier.get() >= maxConcurrentCheckpointAttempts) {
			return Optional.of(queuedRequests.first())
				.filter(CheckpointTriggerRequest::isForce)
				.map(unused -> queuedRequests.pollFirst());
		}

		long nextTriggerDelayMillis = nextTriggerDelayMillis(lastCompletionMs);
		if (nextTriggerDelayMillis > 0) {
			return onTooEarly(nextTriggerDelayMillis);
		}

		return Optional.of(queuedRequests.pollFirst());
	}

	private Optional<CheckpointTriggerRequest> onTooEarly(long nextTriggerDelayMillis) {
		CheckpointTriggerRequest first = queuedRequests.first();
		if (first.isForce()) {
			return Optional.of(queuedRequests.pollFirst());
		} else if (first.isPeriodic) {
			queuedRequests.pollFirst().completeExceptionally(new CheckpointException(MINIMUM_TIME_BETWEEN_CHECKPOINTS));
			rescheduleTrigger.accept(nextTriggerDelayMillis);
			return Optional.empty();
		} else {
			return Optional.empty();
		}
	}

	private long nextTriggerDelayMillis(long lastCheckpointCompletionRelativeTime) {
		return lastCheckpointCompletionRelativeTime - clock.relativeTimeMillis() + minPauseBetweenCheckpoints;
	}

	@VisibleForTesting
	@Deprecated
	PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
		synchronized (lock) {
			return new PriorityQueue<>(queuedRequests);
		}
	}

	void abortAll(CheckpointException exception) {
		Preconditions.checkState(Thread.holdsLock(lock));
		while (!queuedRequests.isEmpty()) {
			queuedRequests.pollFirst().completeExceptionally(exception);
		}
	}

	int getNumQueuedRequests() {
		synchronized (lock) {
			return queuedRequests.size();
		}
	}

	private static Comparator<CheckpointTriggerRequest> checkpointTriggerRequestsComparator() {
		return (r1, r2) -> {
			if (r1.props.isSavepoint() != r2.props.isSavepoint()) {
				return r1.props.isSavepoint() ? -1 : 1;
			} else if (r1.isForce() != r2.isForce()) {
				return r1.isForce() ? -1 : 1;
			} else if (r1.isPeriodic != r2.isPeriodic) {
				return r1.isPeriodic ? 1 : -1;
			} else if (r1.timestamp != r2.timestamp) {
				return Long.compare(r1.timestamp, r2.timestamp);
			} else {
				return Integer.compare(identityHashCode(r1), identityHashCode(r2));
			}
		};
	}

	private static void logInQueueTime(CheckpointTriggerRequest request) {
		if (LOG.isInfoEnabled()) {
			long timeInQueue = request.timestamp - currentTimeMillis();
			if (timeInQueue > LOG_TIME_IN_QUEUE_THRESHOLD_MS) {
				LOG.info("checkpoint request time in queue: {}", timeInQueue);
			}
		}
	}
}
```
</details>

在获得要执行的checkpointing请求后，由```CheckpointCoordinator#startTriggeringCheckpoint```启动checkpointing：

```java
// CheckpointCoordinator.class第506行
private void startTriggeringCheckpoint(CheckpointTriggerRequest request) {
	try {
		synchronized (lock) {
			preCheckGlobalState(request.isPeriodic);
		}

		final Execution[] executions = getTriggerExecutions();
		final Map<ExecutionAttemptID, ExecutionVertex> ackTasks = getAckTasks();

		// we will actually trigger this checkpoint!
		Preconditions.checkState(!isTriggering);
		isTriggering = true;

		final long timestamp = System.currentTimeMillis();
		final CompletableFuture<PendingCheckpoint> pendingCheckpointCompletableFuture =
			initializeCheckpoint(request.props, request.externalSavepointLocation)
				.thenApplyAsync(
					(checkpointIdAndStorageLocation) -> createPendingCheckpoint(
						timestamp,
						request.props,
						ackTasks,
						request.isPeriodic,
						checkpointIdAndStorageLocation.checkpointId,
						checkpointIdAndStorageLocation.checkpointStorageLocation,
						request.getOnCompletionFuture()),
					timer);

		final CompletableFuture<?> masterStatesComplete = pendingCheckpointCompletableFuture
				.thenCompose(this::snapshotMasterState);

		final CompletableFuture<?> coordinatorCheckpointsComplete = pendingCheckpointCompletableFuture
				.thenComposeAsync((pendingCheckpoint) ->
						OperatorCoordinatorCheckpoints.triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
								coordinatorsToCheckpoint, pendingCheckpoint, timer),
						timer);

		FutureUtils.assertNoException(
			CompletableFuture.allOf(masterStatesComplete, coordinatorCheckpointsComplete)
				.handleAsync(
					(ignored, throwable) -> {
						final PendingCheckpoint checkpoint =
							FutureUtils.getWithoutException(pendingCheckpointCompletableFuture);

						Preconditions.checkState(
							checkpoint != null || throwable != null,
							"Either the pending checkpoint needs to be created or an error must have been occurred.");

						if (throwable != null) {
							// the initialization might not be finished yet
							if (checkpoint == null) {
								onTriggerFailure(request, throwable);
							} else {
								onTriggerFailure(checkpoint, throwable);
							}
						} else {
							if (checkpoint.isDiscarded()) {
								onTriggerFailure(
									checkpoint,
									new CheckpointException(
										CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
										checkpoint.getFailureCause()));
							} else {
								// no exception, no discarding, everything is OK
								final long checkpointId = checkpoint.getCheckpointId();
								snapshotTaskState(
									timestamp,
									checkpointId,
									checkpoint.getCheckpointStorageLocation(),
									request.props,
									executions,
									request.advanceToEndOfTime);

								coordinatorsToCheckpoint.forEach((ctx) -> ctx.afterSourceBarrierInjection(checkpointId));

								onTriggerSuccess();
							}
						}

						return null;
					},
					timer)
				.exceptionally(error -> {
					if (!isShutdown()) {
						throw new CompletionException(error);
					} else if (error instanceof RejectedExecutionException) {
						LOG.debug("Execution rejected during shutdown");
					} else {
						LOG.warn("Error encountered during shutdown", error);
					}
					return null;
				}));
	} catch (Throwable throwable) {
		onTriggerFailure(request, throwable);
	}
}
```

启动checkpointing的整个过程是异步的，包括以下几个步骤：

1. 通过```initializeCheckpoint```方法初始化checkpoint并通过```createPendingCheckpoint```方法构造一个pendingCheckpoint（已经启动、但没有收到所有task ack的checkpoint）
2. 通过```snapshotMasterState```方法快照master的状态
3. （与2不分先后）通过```OperatorCoordinatorCheckpoints#triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion```通知并触发算子协调器的checkpointing流程
4. 通过```snapshotTaskState```方法使用RPC调用每个source算子的task executor执行```Task#triggerCheckpointBarrier```
5. 调用所有算子协调器checkpoint上下文的```OperatorCoordinatorCheckpointContext#afterSourceBarrierInjection```方法

### 初始化checkpoint

初始化checkpoint包括```initializeCheckpoint```和```createPendingCheckpoint```两个部分，其中前者负责确定checkpoint的ID和存储位置```CheckpointIdAndStorageLocation```(POJO)；后者创建一个```PendingCheckpoint```类实例，保存整个checkpointing过程中checkpoint元数据。```PendingCheckpoint```包含的元数据如下：

<details>
<summary>PendingCheckpoint实例变量</summary>

```java
// PendingCheckpoint.class第93行
private final Object lock = new Object();

private final JobID jobId;

private final long checkpointId;

private final long checkpointTimestamp;

private final Map<OperatorID, OperatorState> operatorStates;

private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

private final List<MasterState> masterStates;

private final Set<String> notYetAcknowledgedMasterStates;

private final Set<ExecutionAttemptID> acknowledgedTasks;

private final CheckpointProperties props;

private final CheckpointStorageLocation targetLocation;

private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

private final Executor executor;

private int numAcknowledgedTasks;

private boolean discarded;

@Nullable
private PendingCheckpointStats statsCallback;

private volatile ScheduledFuture<?> cancellerHandle;

private CheckpointException failureCause;
```
</details>


这些元数据从变量名中基本能看出其对应的信息，值得注意的是```operatorStates```。这个Map保存的```OperatorState```并不是在State系列前两篇中提到Operator State，而是一个算子在物理层面上的状态，包括该算子在实际运行中各个并行子任务实例的Operator State和Keyed State快照：

<details>
<summary>OperatorState实例变量和OperatorSubtaskState实例变量</summary>

```java
// OperatorState.class第46行
private final OperatorID operatorID;

private final Map<Integer, OperatorSubtaskState> operatorSubtaskStates;

@Nullable
private ByteStreamStateHandle coordinatorState;

private final int parallelism;

private final int maxParallelism;

// OperatorSubtaskState.class第67行
@Nonnull
private final StateObjectCollection<OperatorStateHandle> managedOperatorState;

/**
 * Snapshot written using {@link org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
 */
@Nonnull
private final StateObjectCollection<OperatorStateHandle> rawOperatorState;

/**
 * Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}.
 */
@Nonnull
private final StateObjectCollection<KeyedStateHandle> managedKeyedState;

/**
 * Snapshot written using {@link org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
 */
@Nonnull
private final StateObjectCollection<KeyedStateHandle> rawKeyedState;

@Nonnull
private final StateObjectCollection<InputChannelStateHandle> inputChannelState;

@Nonnull
private final StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState;

/**
 * The state size. This is also part of the deserialized state handle.
 * We store it here in order to not deserialize the state handle when
 * gathering stats.
 */
private final long stateSize;
```
</details>

### 触发Master钩子

```snapshotMasterState```方法会触发所有Master钩子的```triggerCheckpoint```方法。当一个UDF的Source算子使用```ExternallyInducedSource```接口实现的UDF source方法，在创建计算流图时会将调用```WithMasterCheckpointHook#createMasterTriggerRestoreHook```方法创建一个Master钩子，并添加到```CheckpointCoordinator```中。```ExternallyInducedSource```接口的实现在系统Checkpointing时不会触发checkpoint，而是根据从数据源接收到的数据/元素决定何时触发一个checkpoint。因此，在Flink进行checkpointing时，Flink会使用钩子要求数据源准备一个checkpoint数据/元素，而实际该source触发checkpoint的时间依然是根据收到的数据确定的（如果数据源确实立即准备了一个checkpoint数据/元素，那么实际的checkpoint时间不会与Flink的checkpointing结束相差很多）。

<details>
<summary>snapshotMasterState</summary>

```java
// CheckpointCoordinator.class第696行
private CompletableFuture<Void> snapshotMasterState(PendingCheckpoint checkpoint) {
	if (masterHooks.isEmpty()) {
		return CompletableFuture.completedFuture(null);
	}

	final long checkpointID = checkpoint.getCheckpointId();
	final long timestamp = checkpoint.getCheckpointTimestamp();

	final CompletableFuture<Void> masterStateCompletableFuture = new CompletableFuture<>();
	for (MasterTriggerRestoreHook<?> masterHook : masterHooks.values()) {
		MasterHooks
			.triggerHook(masterHook, checkpointID, timestamp, executor)
			.whenCompleteAsync(
				(masterState, throwable) -> {
					try {
						synchronized (lock) {
							if (masterStateCompletableFuture.isDone()) {
								return;
							}
							if (checkpoint.isDiscarded()) {
								throw new IllegalStateException(
									"Checkpoint " + checkpointID + " has been discarded");
							}
							if (throwable == null) {
								checkpoint.acknowledgeMasterState(
									masterHook.getIdentifier(), masterState);
								if (checkpoint.areMasterStatesFullyAcknowledged()) {
									masterStateCompletableFuture.complete(null);
								}
							} else {
								masterStateCompletableFuture.completeExceptionally(throwable);
							}
						}
					} catch (Throwable t) {
						masterStateCompletableFuture.completeExceptionally(t);
					}
				},
				timer);
	}
	return masterStateCompletableFuture;
}
```
</details>

### 触发算子协调器

```OperatorCoordinatorCheckpoints#triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion```方法会触发所有算子协调器（位于JobManager中）的checkpointing流程：

<details>
<summary>OperatorCoordinatorCheckpoints</summary>

```java
// OperatorCoordinatorCheckpoints.class
final class OperatorCoordinatorCheckpoints {

	public static CompletableFuture<CoordinatorSnapshot> triggerCoordinatorCheckpoint(
			final OperatorCoordinatorCheckpointContext coordinatorContext,
			final long checkpointId) throws Exception {

		final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
		coordinatorContext.checkpointCoordinator(checkpointId, checkpointFuture);

		return checkpointFuture.thenApply(
				(state) -> new CoordinatorSnapshot(
						coordinatorContext, new ByteStreamStateHandle(coordinatorContext.operatorId().toString(), state))
		);
	}

	public static CompletableFuture<AllCoordinatorSnapshots> triggerAllCoordinatorCheckpoints(
			final Collection<OperatorCoordinatorCheckpointContext> coordinators,
			final long checkpointId) throws Exception {

		final Collection<CompletableFuture<CoordinatorSnapshot>> individualSnapshots = new ArrayList<>(coordinators.size());

		for (final OperatorCoordinatorCheckpointContext coordinator : coordinators) {
			final CompletableFuture<CoordinatorSnapshot> checkpointFuture = triggerCoordinatorCheckpoint(coordinator, checkpointId);
			individualSnapshots.add(checkpointFuture);
		}

		return FutureUtils.combineAll(individualSnapshots).thenApply(AllCoordinatorSnapshots::new);
	}

	public static CompletableFuture<Void> triggerAndAcknowledgeAllCoordinatorCheckpoints(
			final Collection<OperatorCoordinatorCheckpointContext> coordinators,
			final PendingCheckpoint checkpoint,
			final Executor acknowledgeExecutor) throws Exception {

		final CompletableFuture<AllCoordinatorSnapshots> snapshots =
				triggerAllCoordinatorCheckpoints(coordinators, checkpoint.getCheckpointId());

		return snapshots
				.thenAcceptAsync(
						(allSnapshots) -> {
							try {
								acknowledgeAllCoordinators(checkpoint, allSnapshots.snapshots);
							}
							catch (Exception e) {
								throw new CompletionException(e);
							}
						},
						acknowledgeExecutor);
	}

	public static CompletableFuture<Void> triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
			final Collection<OperatorCoordinatorCheckpointContext> coordinators,
			final PendingCheckpoint checkpoint,
			final Executor acknowledgeExecutor) throws CompletionException {

		try {
			return triggerAndAcknowledgeAllCoordinatorCheckpoints(coordinators, checkpoint, acknowledgeExecutor);
		} catch (Exception e) {
			throw new CompletionException(e);
		}
	}

	// ------------------------------------------------------------------------

	private static void acknowledgeAllCoordinators(PendingCheckpoint checkpoint, Collection<CoordinatorSnapshot> snapshots) throws CheckpointException {
		for (final CoordinatorSnapshot snapshot : snapshots) {
			final PendingCheckpoint.TaskAcknowledgeResult result =
				checkpoint.acknowledgeCoordinatorState(snapshot.coordinator, snapshot.state);

			if (result != PendingCheckpoint.TaskAcknowledgeResult.SUCCESS) {
				final String errorMessage = "Coordinator state not acknowledged successfully: " + result;
				final Throwable error = checkpoint.isDiscarded() ? checkpoint.getFailureCause() : null;

				if (error != null) {
					throw new CheckpointException(errorMessage, CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, error);
				} else {
					throw new CheckpointException(errorMessage, CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE);
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	static final class AllCoordinatorSnapshots {

		private final Collection<CoordinatorSnapshot> snapshots;

		AllCoordinatorSnapshots(Collection<CoordinatorSnapshot> snapshots) {
			this.snapshots = snapshots;
		}

		public Iterable<CoordinatorSnapshot> snapshots() {
			return snapshots;
		}
	}

	static final class CoordinatorSnapshot {

		final OperatorInfo coordinator;
		final ByteStreamStateHandle state;

		CoordinatorSnapshot(OperatorInfo coordinator, ByteStreamStateHandle state) {
			this.coordinator = coordinator;
			this.state = state;
		}
	}
}
```
</details>

最终会执行每个算子协调器的```OperatorCoordinatorCheckpointContext#checkpointCoordinator```方法（```OperatorCoordinatorHolder```是该接口的唯一实现类）：

```java
// OperatorCoordinatorHolder.class第202行
public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
	mainThreadExecutor.execute(() -> checkpointCoordinatorInternal(checkpointId, result));
}

// OperatorCoordinatorHolder.class第233行
private void checkpointCoordinatorInternal(final long checkpointId, final CompletableFuture<byte[]> result) {
	mainThreadExecutor.assertRunningInMainThread();

	// synchronously!!!, with the completion, we need to shut the event valve
	result.whenComplete((success, failure) -> {
		if (failure != null) {
			result.completeExceptionally(failure);
		} else {
			try {
				eventValve.shutValve(checkpointId);
				result.complete(success);
			} catch (Exception e) {
				result.completeExceptionally(e);
			}
		}
	});

	try {
		eventValve.markForCheckpoint(checkpointId);
		coordinator.checkpointCoordinator(checkpointId, result);
	} catch (Throwable t) {
		ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
		result.completeExceptionally(t);
		globalFailureHandler.accept(t);
	}
}
```



## Barrier

## Snapshotting

## 完成checkpoint

## Unaligned Barrier

## 参考文献