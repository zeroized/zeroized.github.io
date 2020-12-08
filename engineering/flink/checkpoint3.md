# Checkpoint(3): 执行与完成checkpoint
2020/11/21

前两篇[Checkpoint(1): 启动Checkpoint](/engineering/flink/checkpoint1.md)和[Checkpoint(2): 处理Barrier](/engineering/flink/checkpoint3.md)中介绍了checkpoint的启动和Barrier的处理。本篇是State系列中关于checkpoint的最后一篇，将介绍checkpoint过程中剩余的两个流程：快照的执行过程以及算子完成checkpointing后向```CheckpointCoordinator``` ack的过程。最后将简单汇总Flink1.11版本新加入的Unaligned Barrier机制在每个checkpoint步骤中与aligned Barrier的不同之处。

注：源代码为Flink1.11.0版本

## 算子checkpoint的执行与中止

在前一篇State(3)中提到，算子的checkpointing以```StreamTask#performCheckpoint```方法开始，其中包含5个步骤（即State(3)篇中发出Barrier的流程）：
1. 如果上一个checkpoint是失败的checkpoint，向下游算子广播中止上一个checkpoint的事件```CancelCheckpointMarker```
2. 告知Source以及和Source链接在一起的所有算子准备Barrier前的快照
3. 向下游算子广播Barrier事件```CheckpointBarrier```
4. 如果是Unaligned Barrier，对正在发送过程中的数据元素进行快照
5. 调用```takeSnapshotSync```方法对算子状态进行快照，如果在这个步骤发生错误，清理失败的快照并向```channelStateWriter```发出checkpoint失败消息

source算子的```performCheckpoint```方法由```StreamTask#triggerCheckpoint```触发；其他算子通过```CheckpointBarrierHandle#notifyCheckpoint```调用```StreamTask#triggerCheckpointOnBarrier```触发。而算子的checkpoint中止则是交由```SubtaskCheckpointCoordinatorImpl```的```abortCheckpointOnBarrier```方法执行。

### 算子快照（Snapshotting）

在算子checkpointing的最后一步，即在向下游发出Barrier后进行算子的快照（snapshot）。算子快照的起点是```SubtaskCheckpointCoordinatorImpl#takeSnapshotSync```方法：

<details>
<summary>SubtaskCheckpointCoordinatorImpl#takeSnapshotSync</summary>

```java
// SubtaskCheckpointCoordinatorImpl.class第477行
private boolean takeSnapshotSync(
		Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
		CheckpointMetaData checkpointMetaData,
		CheckpointMetrics checkpointMetrics,
		CheckpointOptions checkpointOptions,
		OperatorChain<?, ?> operatorChain,
		Supplier<Boolean> isCanceled) throws Exception {

	for (final StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
		if (operatorWrapper.isClosed()) {
			env.declineCheckpoint(checkpointMetaData.getCheckpointId(),
				new CheckpointException("Task Name" + taskName, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING));
			return false;
		}
	}

	long checkpointId = checkpointMetaData.getCheckpointId();
	long started = System.nanoTime();

	ChannelStateWriteResult channelStateWriteResult = checkpointOptions.isUnalignedCheckpoint() ?
							channelStateWriter.getAndRemoveWriteResult(checkpointId) :
							ChannelStateWriteResult.EMPTY;

	CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(checkpointId, checkpointOptions.getTargetLocation());

	try {
		for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
			if (!operatorWrapper.isClosed()) {
				operatorSnapshotsInProgress.put(
						operatorWrapper.getStreamOperator().getOperatorID(),
						buildOperatorSnapshotFutures(
								checkpointMetaData,
								checkpointOptions,
								operatorChain,
								operatorWrapper.getStreamOperator(),
								isCanceled,
								channelStateWriteResult,
								storage));
			}
		}
	} finally {
		checkpointStorage.clearCacheFor(checkpointId);
	}

	LOG.debug(
		"{} - finished synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms",
		taskName,
		checkpointId,
		checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
		checkpointMetrics.getSyncDurationMillis());

	checkpointMetrics.setSyncDurationMillis((System.nanoTime() - started) / 1_000_000);
	return true;
}
```
</details>



首先协调器会从尾部开始遍历链接的算子，确认链上所有的算子都处于open状态，否则拒绝这个checkpoint（```Environment#declineCheckpoint```）。然后根据checkpoint的设置获取checkpoint写入目标的通道，从链接算子尾部向头部逐个调用```SubtaskCheckpointCoordinatorImpl#buildOperatorSnapshotFutures```方法异步地构造算子快照：

<details>
<summary>SubtaskCheckpointCoordinatorImpl#buildOperatorSnapshotFutures</summary>

```java
// SubtaskCheckpointCoordinatorImpl.class第532行
private OperatorSnapshotFutures buildOperatorSnapshotFutures(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		OperatorChain<?, ?> operatorChain,
		StreamOperator<?> op,
		Supplier<Boolean> isCanceled,
		ChannelStateWriteResult channelStateWriteResult,
		CheckpointStreamFactory storage) throws Exception {
	OperatorSnapshotFutures snapshotInProgress = checkpointStreamOperator(
		op,
		checkpointMetaData,
		checkpointOptions,
		storage,
		isCanceled);
	if (op == operatorChain.getHeadOperator()) {
		snapshotInProgress.setInputChannelStateFuture(
			channelStateWriteResult
				.getInputChannelStateHandles()
				.thenApply(StateObjectCollection::new)
				.thenApply(SnapshotResult::of));
	}
	if (op == operatorChain.getTailOperator()) {
		snapshotInProgress.setResultSubpartitionStateFuture(
			channelStateWriteResult
				.getResultSubpartitionStateHandles()
				.thenApply(StateObjectCollection::new)
				.thenApply(SnapshotResult::of));
	}
	return snapshotInProgress;
}

// SubtaskCheckpointCoordinatorImpl.class第607行
private static OperatorSnapshotFutures checkpointStreamOperator(
		StreamOperator<?> op,
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		CheckpointStreamFactory storageLocation,
		Supplier<Boolean> isCanceled) throws Exception {
	try {
		return op.snapshotState(
			checkpointMetaData.getCheckpointId(),
			checkpointMetaData.getTimestamp(),
			checkpointOptions,
			storageLocation);
	}
	catch (Exception ex) {
		if (!isCanceled.get()) {
			LOG.info(ex.getMessage(), ex);
		}
		throw ex;
	}
}
```
</details>

在这个过程中，所有的算子都会通过```StreamOperatorStateHandler#snapshotState```方法进行算子中keyed状态和Operator状态的快照。对于头部算子，需要额外向checkpoint目标通道中写入算子输入通道（即接收Buffer和Barrier的通道）的状态；对尾部算子则需要额外写入算子结果子分片（即输出Buffer和Barrier的通道）的状态。

<details>
<summary>StreamOperatorStateHandler#snapshotState</summary>

```java
// StreamOperatorStateHandler.class第136行
public OperatorSnapshotFutures snapshotState(
		CheckpointedStreamOperator streamOperator,
		Optional<InternalTimeServiceManager<?>> timeServiceManager,
		String operatorName,
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions,
		CheckpointStreamFactory factory) throws CheckpointException {
	KeyGroupRange keyGroupRange = null != keyedStateBackend ?
		keyedStateBackend.getKeyGroupRange() : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

	OperatorSnapshotFutures snapshotInProgress = new OperatorSnapshotFutures();

	StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl(
		checkpointId,
		timestamp,
		factory,
		keyGroupRange,
		closeableRegistry);

	snapshotState(
		streamOperator,
		timeServiceManager,
		operatorName,
		checkpointId,
		timestamp,
		checkpointOptions,
		factory,
		snapshotInProgress,
		snapshotContext);

	return snapshotInProgress;
}

@VisibleForTesting
void snapshotState(
		CheckpointedStreamOperator streamOperator,
		Optional<InternalTimeServiceManager<?>> timeServiceManager,
		String operatorName,
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions,
		CheckpointStreamFactory factory,
		OperatorSnapshotFutures snapshotInProgress,
		StateSnapshotContextSynchronousImpl snapshotContext) throws CheckpointException {
	try {
		if (timeServiceManager.isPresent()) {
			checkState(keyedStateBackend != null, "keyedStateBackend should be available with timeServiceManager");
			timeServiceManager.get().snapshotState(keyedStateBackend, snapshotContext, operatorName);
		}
		streamOperator.snapshotState(snapshotContext);

		snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
		snapshotInProgress.setOperatorStateRawFuture(snapshotContext.getOperatorStateStreamFuture());

		if (null != operatorStateBackend) {
			snapshotInProgress.setOperatorStateManagedFuture(
				operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
		}

		if (null != keyedStateBackend) {
			snapshotInProgress.setKeyedStateManagedFuture(
				keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
		}
	} catch (Exception snapshotException) {
		try {
			snapshotInProgress.cancel();
		} catch (Exception e) {
			snapshotException.addSuppressed(e);
		}

		String snapshotFailMessage = "Could not complete snapshot " + checkpointId + " for operator " +
			operatorName + ".";

		try {
			snapshotContext.closeExceptionally();
		} catch (IOException e) {
			snapshotException.addSuppressed(e);
		}
		throw new CheckpointException(snapshotFailMessage, CheckpointFailureReason.CHECKPOINT_DECLINED, snapshotException);
	}
}
```
</details>

算子的快照共包含4个部分：
- 快照时间服务
- 算子自定义快照逻辑（```AbstractStreamOperator#snapshotState```是空实现，各个算子override这个方法实现自己的额外快照逻辑）
- 快照算子状态并写入后端
- 快照keyed状态并写入后端

### Unaligned算子快照（施工中）

## 中止checkpoint

在两种情况下一个算子checkpoint会被中止：一种是当算子收到CancelCheckpointMarker时或是在checkpointing时发生错误；还有一种是收到中止checkpoint的通知（来自```CheckpointCoordinator```）。第一种情况下，```CheckpointCoordinator```并不知道

#### Abort on Cancel-Barrier

当```CheckpointBarrierHandler```（Tracker、Aligner和Unaligner）收到```CancellationBarrier```（即```CancelCheckpointMarker```）或是在处理```CheckpointBarrier```时发生错误，就会通过```notifyAbortOnCancellationBarrier```方法间接或是直接调用```notifyAbort```方法进行中止checkpoint的流程。该流程的实际处理部分位于```SubtaskCheckpointCoordinatorImpl#abortCheckpointOnBarrier```：

```java
// SubtaskCheckpointCoordinatorImpl.class第181行
public void abortCheckpointOnBarrier(long checkpointId, Throwable cause, OperatorChain<?, ?> operatorChain) throws IOException {
	LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, taskName);
	lastCheckpointId = Math.max(lastCheckpointId, checkpointId);
	Iterator<Long> iterator = abortedCheckpointIds.iterator();
	while (iterator.hasNext()) {
		long next = iterator.next();
		if (next < lastCheckpointId) {
			iterator.remove();
		} else {
			break;
		}
	}

	checkpointStorage.clearCacheFor(checkpointId);

	channelStateWriter.abort(checkpointId, cause, true);

	// notify the coordinator that we decline this checkpoint
	env.declineCheckpoint(checkpointId, cause);

	// notify all downstream operators that they should not wait for a barrier from us
	actionExecutor.runThrowing(() -> operatorChain.broadcastEvent(new CancelCheckpointMarker(checkpointId)));
}
```

```SubtaskCheckpointCoordinatorImpl```维护了名为```abortedCheckpointIds```的Set用于记录被中止的checkpoint（Abort on Notification时，如果要中止的checkpoint没有被触发，就会向这个Set中添加一个记录）。
执行中止checkpoint流程如下：
1. 移除abortedCheckpointIds中比待中止的checkpoint更早的元素
2. 移除该checkpoint存储的缓存
3. 中止该checkpoint的通道状态写入
4. 告知协调器拒绝了checkpoint
5. 向下游发出Cancel-Barrier

#### Abort on Notification

另一种中止checkpoint的情况是来自```CheckpointCoordinator```的abort通知。```CheckpointCoordinator```在启动checkpointing时或是在完成checkpoint时发生错误都会通过```CheckpointCoordinator#sendAbortedMessages```方法向所有需要commit的task发出中止checkpoint的通知。

```java
// SubtaskCheckpointCoordinatorImpl.class第292行
public void notifyCheckpointAborted(long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning) throws Exception {

	Exception previousException = null;
	if (isRunning.get()) {
		LOG.debug("Notification of aborted checkpoint for task {}", taskName);

		boolean canceled = cancelAsyncCheckpointRunnable(checkpointId);

		if (!canceled) {
			if (checkpointId > lastCheckpointId) {
				// only record checkpoints that have not triggered on task side.
				abortedCheckpointIds.add(checkpointId);
			}
		}

		channelStateWriter.abort(checkpointId, new CancellationException("checkpoint aborted via notification"), false);

		for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
			try {
				operatorWrapper.getStreamOperator().notifyCheckpointAborted(checkpointId);
			} catch (Exception e) {
				previousException = e;
			}
		}

	} else {
		LOG.debug("Ignoring notification of aborted checkpoint for not-running task {}", taskName);
	}

	env.getTaskStateManager().notifyCheckpointAborted(checkpointId);
	ExceptionUtils.tryRethrowException(previousException);
}
```

Abort on Notification的流程分为以下四步：
1. 协调器会试图取消checkpoint，如果取消失败且待取消的checkpoint id大于上一个checkpoint（也就是该checkpoint还没触发），就在abortedCheckpointIds中记录这个checkpoint
2. 中止该checkpoint的通道状态写入
3. 通知链上每个算子执行```StreamOperator#notifyCheckpointAborted```
4. 通知TaskStateManager中止本地状态存储的checkpoint

## 完成checkpoint

### 算子ack与ack的处理

算子在执行快照后（算子checkpointing第五步），如果快照没有发生异常，会调用```finishAndReportAsync```方法向```CheckpointCoordinator```进行ack，由一个```AsyncCheckpointRunnable```异步进行：

<details>
<summary>AsyncCheckpointRunnable</summary>

```java
// AsyncCheckpointRunnable.class
final class AsyncCheckpointRunnable implements Runnable, Closeable {

	public static final Logger LOG = LoggerFactory.getLogger(AsyncCheckpointRunnable.class);
	private final String taskName;
	private final Consumer<AsyncCheckpointRunnable> registerConsumer;
	private final Consumer<AsyncCheckpointRunnable> unregisterConsumer;
	private final Environment taskEnvironment;

	enum AsyncCheckpointState {
		RUNNING,
		DISCARDED,
		COMPLETED
	}

	private final AsyncExceptionHandler asyncExceptionHandler;
	private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;
	private final CheckpointMetaData checkpointMetaData;
	private final CheckpointMetrics checkpointMetrics;
	private final long asyncStartNanos;
	private final AtomicReference<AsyncCheckpointState> asyncCheckpointState = new AtomicReference<>(AsyncCheckpointState.RUNNING);

	AsyncCheckpointRunnable(
			Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
			CheckpointMetaData checkpointMetaData,
			CheckpointMetrics checkpointMetrics,
			long asyncStartNanos,
			String taskName,
			Consumer<AsyncCheckpointRunnable> register,
			Consumer<AsyncCheckpointRunnable> unregister,
			Environment taskEnvironment,
			AsyncExceptionHandler asyncExceptionHandler) {

		this.operatorSnapshotsInProgress = checkNotNull(operatorSnapshotsInProgress);
		this.checkpointMetaData = checkNotNull(checkpointMetaData);
		this.checkpointMetrics = checkNotNull(checkpointMetrics);
		this.asyncStartNanos = asyncStartNanos;
		this.taskName = checkNotNull(taskName);
		this.registerConsumer = register;
		this.unregisterConsumer = unregister;
		this.taskEnvironment = checkNotNull(taskEnvironment);
		this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
	}

	@Override
	public void run() {
		FileSystemSafetyNet.initializeSafetyNetForThread();
		try {
			registerConsumer.accept(this);

			TaskStateSnapshot jobManagerTaskOperatorSubtaskStates = new TaskStateSnapshot(operatorSnapshotsInProgress.size());
			TaskStateSnapshot localTaskOperatorSubtaskStates = new TaskStateSnapshot(operatorSnapshotsInProgress.size());

			for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry : operatorSnapshotsInProgress.entrySet()) {

				OperatorID operatorID = entry.getKey();
				OperatorSnapshotFutures snapshotInProgress = entry.getValue();

				// finalize the async part of all by executing all snapshot runnables
				OperatorSnapshotFinalizer finalizedSnapshots =
					new OperatorSnapshotFinalizer(snapshotInProgress);

				jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
					operatorID,
					finalizedSnapshots.getJobManagerOwnedState());

				localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
					operatorID,
					finalizedSnapshots.getTaskLocalState());
			}

			final long asyncEndNanos = System.nanoTime();
			final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000L;

			checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

			if (asyncCheckpointState.compareAndSet(AsyncCheckpointState.RUNNING, AsyncCheckpointState.COMPLETED)) {

				reportCompletedSnapshotStates(
					jobManagerTaskOperatorSubtaskStates,
					localTaskOperatorSubtaskStates,
					asyncDurationMillis);

			} else {
				LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
					taskName,
					checkpointMetaData.getCheckpointId());
			}
		} catch (Exception e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("{} - asynchronous part of checkpoint {} could not be completed.",
					taskName,
					checkpointMetaData.getCheckpointId(),
					e);
			}
			handleExecutionException(e);
		} finally {
			unregisterConsumer.accept(this);
			FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
		}
	}

	private void reportCompletedSnapshotStates(
		TaskStateSnapshot acknowledgedTaskStateSnapshot,
		TaskStateSnapshot localTaskStateSnapshot,
		long asyncDurationMillis) {

		boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
		boolean hasLocalState = localTaskStateSnapshot.hasState();

		Preconditions.checkState(hasAckState || !hasLocalState,
			"Found cached state but no corresponding primary state is reported to the job " +
				"manager. This indicates a problem.");

		// we signal stateless tasks by reporting null, so that there are no attempts to assign empty state
		// to stateless tasks on restore. This enables simple job modifications that only concern
		// stateless without the need to assign them uids to match their (always empty) states.
		taskEnvironment.getTaskStateManager().reportTaskStateSnapshots(
			checkpointMetaData,
			checkpointMetrics,
			hasAckState ? acknowledgedTaskStateSnapshot : null,
			hasLocalState ? localTaskStateSnapshot : null);

		LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
			taskName, checkpointMetaData.getCheckpointId(), asyncDurationMillis);

		LOG.trace("{} - reported the following states in snapshot for checkpoint {}: {}.",
			taskName, checkpointMetaData.getCheckpointId(), acknowledgedTaskStateSnapshot);
	}

	private void handleExecutionException(Exception e) {

		boolean didCleanup = false;
		AsyncCheckpointState currentState = asyncCheckpointState.get();

		while (AsyncCheckpointState.DISCARDED != currentState) {

			if (asyncCheckpointState.compareAndSet(currentState, AsyncCheckpointState.DISCARDED)) {

				didCleanup = true;

				try {
					cleanup();
				} catch (Exception cleanupException) {
					e.addSuppressed(cleanupException);
				}

				Exception checkpointException = new Exception(
					"Could not materialize checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
						taskName + '.',
					e);

				// We only report the exception for the original cause of fail and cleanup.
				// Otherwise this followup exception could race the original exception in failing the task.
				try {
					taskEnvironment.declineCheckpoint(checkpointMetaData.getCheckpointId(), checkpointException);
				} catch (Exception unhandled) {
					AsynchronousException asyncException = new AsynchronousException(unhandled);
					asyncExceptionHandler.handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
				}

				currentState = AsyncCheckpointState.DISCARDED;
			} else {
				currentState = asyncCheckpointState.get();
			}
		}

		if (!didCleanup) {
			LOG.trace("Caught followup exception from a failed checkpoint thread. This can be ignored.", e);
		}
	}

	@Override
	public void close() {
		if (asyncCheckpointState.compareAndSet(AsyncCheckpointState.RUNNING, AsyncCheckpointState.DISCARDED)) {

			try {
				cleanup();
			} catch (Exception cleanupException) {
				LOG.warn("Could not properly clean up the async checkpoint runnable.", cleanupException);
			}
		} else {
			logFailedCleanupAttempt();
		}
	}

	long getCheckpointId() {
		return checkpointMetaData.getCheckpointId();
	}

	private void cleanup() throws Exception {
		LOG.debug(
			"Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.",
			checkpointMetaData.getCheckpointId(),
			taskName);

		Exception exception = null;

		// clean up ongoing operator snapshot results and non partitioned state handles
		for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
			if (operatorSnapshotResult != null) {
				try {
					operatorSnapshotResult.cancel();
				} catch (Exception cancelException) {
					exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
				}
			}
		}

		if (null != exception) {
			throw exception;
		}
	}

	private void logFailedCleanupAttempt() {
		LOG.debug("{} - asynchronous checkpointing operation for checkpoint {} has " +
				"already been completed. Thus, the state handles are not cleaned up.",
			taskName,
			checkpointMetaData.getCheckpointId());
	}

}
```
</details>

如果ack的过程没有发生异常，```AsyncCheckpointRunnable```会通过RPC通知```CheckpointCoordinator```：
- ```TaskStateManagerImpl#reportTaskStateSnapshots```
- ```RpcCheckpointResponder#acknowledgeCheckpoint```
- ```JobMaster#acknowledgeCheckpoint```
- ```ScheduleBase#acknowledgeCheckpoint```
- ```CheckpointCoordinator#receiveAcknowledgeMessage```

<details>
<summary>CheckpointCoordinator#receiveAcknowledgeMessage</summary>

```java
public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message, String taskManagerLocationInfo) throws CheckpointException {
	if (shutdown || message == null) {
		return false;
	}

	if (!job.equals(message.getJob())) {
		LOG.error("Received wrong AcknowledgeCheckpoint message for job {} from {} : {}", job, taskManagerLocationInfo, message);
		return false;
	}

	final long checkpointId = message.getCheckpointId();

	synchronized (lock) {
		// we need to check inside the lock for being shutdown as well, otherwise we
		// get races and invalid error log messages
		if (shutdown) {
			return false;
		}

		final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

		if (checkpoint != null && !checkpoint.isDiscarded()) {

			switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getSubtaskState(), message.getCheckpointMetrics())) {
				case SUCCESS:
					LOG.debug("Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
						checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

					if (checkpoint.areTasksFullyAcknowledged()) {
						completePendingCheckpoint(checkpoint);
					}
					break;
				case DUPLICATE:
					LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
						message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
					break;
				case UNKNOWN:
					LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
							"because the task's execution attempt id was unknown. Discarding " +
							"the state handle to avoid lingering state.", message.getCheckpointId(),
						message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

					discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

					break;
				case DISCARDED:
					LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
						"because the pending checkpoint had been discarded. Discarding the " +
							"state handle tp avoid lingering state.",
						message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

					discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());
			}

			return true;
		}
		else if (checkpoint != null) {
			// this should not happen
			throw new IllegalStateException(
					"Received message for discarded but non-removed checkpoint " + checkpointId);
		}
		else {
			boolean wasPendingCheckpoint;

			// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
			if (recentPendingCheckpoints.contains(checkpointId)) {
				wasPendingCheckpoint = true;
				LOG.warn("Received late message for now expired checkpoint attempt {} from task " +
					"{} of job {} at {}.", checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
			}
			else {
				LOG.debug("Received message for an unknown checkpoint {} from task {} of job {} at {}.",
					checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
				wasPendingCheckpoint = false;
			}

			// try to discard the state so that we don't have lingering state lying around
			discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

			return wasPendingCheckpoint;
		}
	}
}
```
</details>

在```CheckpointCoordinator```收到算子的ack后，首先会放到对应的```pendingCheckpoint```中判断这个ack的状态：

<details>
<summary>PendingCheckpoint#acknowledgeTask</summary>

```java
// PendingCheckpoint.class第354行
public TaskAcknowledgeResult acknowledgeTask(
		ExecutionAttemptID executionAttemptId,
		TaskStateSnapshot operatorSubtaskStates,
		CheckpointMetrics metrics) {

	synchronized (lock) {
		if (discarded) {
			return TaskAcknowledgeResult.DISCARDED;
		}

		final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

		if (vertex == null) {
			if (acknowledgedTasks.contains(executionAttemptId)) {
				return TaskAcknowledgeResult.DUPLICATE;
			} else {
				return TaskAcknowledgeResult.UNKNOWN;
			}
		} else {
			acknowledgedTasks.add(executionAttemptId);
		}

		List<OperatorIDPair> operatorIDs = vertex.getJobVertex().getOperatorIDs();
		int subtaskIndex = vertex.getParallelSubtaskIndex();
		long ackTimestamp = System.currentTimeMillis();

		long stateSize = 0L;

		if (operatorSubtaskStates != null) {
			for (OperatorIDPair operatorID : operatorIDs) {

				OperatorSubtaskState operatorSubtaskState =
					operatorSubtaskStates.getSubtaskStateByOperatorID(operatorID.getGeneratedOperatorID());

				// if no real operatorSubtaskState was reported, we insert an empty state
				if (operatorSubtaskState == null) {
					operatorSubtaskState = new OperatorSubtaskState();
				}

				OperatorState operatorState = operatorStates.get(operatorID.getGeneratedOperatorID());

				if (operatorState == null) {
					operatorState = new OperatorState(
						operatorID.getGeneratedOperatorID(),
						vertex.getTotalNumberOfParallelSubtasks(),
						vertex.getMaxParallelism());
					operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
				}

				operatorState.putState(subtaskIndex, operatorSubtaskState);
				stateSize += operatorSubtaskState.getStateSize();
			}
		}

		++numAcknowledgedTasks;

		// publish the checkpoint statistics
		// to prevent null-pointers from concurrent modification, copy reference onto stack
		final PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			// Do this in millis because the web frontend works with them
			long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
			long checkpointStartDelayMillis = metrics.getCheckpointStartDelayNanos() / 1_000_000;

			SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
				subtaskIndex,
				ackTimestamp,
				stateSize,
				metrics.getSyncDurationMillis(),
				metrics.getAsyncDurationMillis(),
				alignmentDurationMillis,
				checkpointStartDelayMillis);

			statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats);
		}

		return TaskAcknowledgeResult.SUCCESS;
	}
}
```
</details>

```pendingCheckpoint```根据收到的ack对应的task是否属于```notYetAcknowledgedTasks```判断是否是一个有效的ack，如果是一个有效的ack，则在```acknowledgedTasks```中添加这个task（并将已完成checkpoint的task计数器加一）；否则根据这个ack对应的task是否属于```acknowledgedTasks```返回“重复ack”或是“未知ack”。然后将ack中的subtask状态和指标记录到```operatorStates```中，最后向负责统计checkpoint数据的回调方法写入该subtask的数据并返回“成功ack”。

如果此时pendingCheckpoint的```notYetAcknowledgedTasks```已经为空，则开始完成checkpointing的流程。

### 完成checkpointing

在```pendingCheckpoint```中所有的task都完成checkpoint以后，执行```completePendingCheckpoint```完成整个checkpointing流程。

<details>
<summary>CheckpointCoordinator#completePendingCheckpoint</summary>

```java
// CheckpointCoordinator.class第1011行
private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException {
	final long checkpointId = pendingCheckpoint.getCheckpointId();
	final CompletedCheckpoint completedCheckpoint;

	// As a first step to complete the checkpoint, we register its state with the registry
	Map<OperatorID, OperatorState> operatorStates = pendingCheckpoint.getOperatorStates();
	sharedStateRegistry.registerAll(operatorStates.values());

	try {
		try {
			completedCheckpoint = pendingCheckpoint.finalizeCheckpoint();
			failureManager.handleCheckpointSuccess(pendingCheckpoint.getCheckpointId());
		}
		catch (Exception e1) {
			// abort the current pending checkpoint if we fails to finalize the pending checkpoint.
			if (!pendingCheckpoint.isDiscarded()) {
				abortPendingCheckpoint(
					pendingCheckpoint,
					new CheckpointException(
						CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1));
			}

			throw new CheckpointException("Could not finalize the pending checkpoint " + checkpointId + '.',
				CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1);
		}

		// the pending checkpoint must be discarded after the finalization
		Preconditions.checkState(pendingCheckpoint.isDiscarded() && completedCheckpoint != null);

		try {
			completedCheckpointStore.addCheckpoint(completedCheckpoint);
		} catch (Exception exception) {
			// we failed to store the completed checkpoint. Let's clean up
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						completedCheckpoint.discardOnFailedStoring();
					} catch (Throwable t) {
						LOG.warn("Could not properly discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), t);
					}
				}
			});

			sendAbortedMessages(checkpointId, pendingCheckpoint.getCheckpointTimestamp());
			throw new CheckpointException("Could not complete the pending checkpoint " + checkpointId + '.',
				CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, exception);
		}
	} finally {
		pendingCheckpoints.remove(checkpointId);
		timer.execute(this::executeQueuedRequest);
	}

	rememberRecentCheckpointId(checkpointId);

	// drop those pending checkpoints that are at prior to the completed one
	dropSubsumedCheckpoints(checkpointId);

	// record the time when this was completed, to calculate
	// the 'min delay between checkpoints'
	lastCheckpointCompletionRelativeTime = clock.relativeTimeMillis();

	LOG.info("Completed checkpoint {} for job {} ({} bytes in {} ms).", checkpointId, job,
		completedCheckpoint.getStateSize(), completedCheckpoint.getDuration());

	if (LOG.isDebugEnabled()) {
		StringBuilder builder = new StringBuilder();
		builder.append("Checkpoint state: ");
		for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
			builder.append(state);
			builder.append(", ");
		}
		// Remove last two chars ", "
		builder.setLength(builder.length() - 2);

		LOG.debug(builder.toString());
	}

	// send the "notify complete" call to all vertices, coordinators, etc.
	sendAcknowledgeMessages(checkpointId, completedCheckpoint.getTimestamp());
}
```
</details>

首先将pendingCheckpoint中所有的算子状态注册到```sharedStateRegistry```中，然后通过```PendingCheckpoint#finalizeCheckpoint```将```PendingCheckpoint```转换成一个```CompleteCheckpoint```：

<details>
<summary>PendingCheckpoint#finalizeCheckpoint</summary>

```java
public CompletedCheckpoint finalizeCheckpoint() throws IOException {

	synchronized (lock) {
		checkState(!isDiscarded(), "checkpoint is discarded");
		checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet");

		// make sure we fulfill the promise with an exception if something fails
		try {
			// write out the metadata
			final CheckpointMetadata savepoint = new CheckpointMetadata(checkpointId, operatorStates.values(), masterStates);
			final CompletedCheckpointStorageLocation finalizedLocation;

			try (CheckpointMetadataOutputStream out = targetLocation.createMetadataOutputStream()) {
				Checkpoints.storeCheckpointMetadata(savepoint, out);
				finalizedLocation = out.closeAndFinalizeCheckpoint();
			}

			CompletedCheckpoint completed = new CompletedCheckpoint(
					jobId,
					checkpointId,
					checkpointTimestamp,
					System.currentTimeMillis(),
					operatorStates,
					masterStates,
					props,
					finalizedLocation);

			onCompletionPromise.complete(completed);

			// to prevent null-pointers from concurrent modification, copy reference onto stack
			PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				// Finalize the statsCallback and give the completed checkpoint a
				// callback for discards.
				CompletedCheckpointStats.DiscardCallback discardCallback =
						statsCallback.reportCompletedCheckpoint(finalizedLocation.getExternalPointer());
				completed.setDiscardCallback(discardCallback);
			}

			// mark this pending checkpoint as disposed, but do NOT drop the state
			dispose(false);

			return completed;
		}
		catch (Throwable t) {
			onCompletionPromise.completeExceptionally(t);
			ExceptionUtils.rethrowIOException(t);
			return null; // silence the compiler
		}
	}
}
```
</details>

协调器会试图将```completeCheckpoint```写入```completedCheckpointStore```存储（Standalone或是Zookeeper），如果此时发生错误会通过```sendAbortedMessages```通知所有的算子中止checkpoint。写入完成后会从```pendingCheckpoints```表中移除该checkpoint，然后在timer启动一个新的checkpoint。在记录checkpoint信息和丢弃失败未被清理的checkpoint后，在完成checkpointing的最后阶段，```CheckpointCoordinator```通过```sendAcknowledgeMessages```方法向所有的算子和协调器发出checkpoint完成的通知（见```SubtaskCheckpointCoordinatorImpl#notifyCheckpointComplete```和```SourceCoordinator#checkpointComplete```）。

### 拒绝checkpoint

拒绝checkpoint发生于任一算子checkpointing错误时，包括Task在触发CheckpointBarrier处理时发生错误（Task不在运行），算子Abort on Cancel-Barrier、算子ack错误和checkpoint完成错误。此时此时JobManager中的```CheckpointCoordinator```依然在等待所有算子响应，因此在其中止checkpoint的第四步调用```RuntimeEnvironment#declineCheckpoint```通过RPC通知```CheckpointCoordinator```。整个RPC经过的方法和算子ack的链路基本一致：
- ```RuntimeEnvironment#declineCheckpoint```
- ```RpcCheckpointResponder#declineCheckpoint```
- ```JobMaster#declineCheckpoint```
- ```ScheduleBase#declineCheckpoint```
- ```CheckpointCoordinator#receiveDeclineMessage```

<details>
<summary>CheckpointCoordinator#receiveDeclineMessage</summary>

```java
// CheckpointCoordinator.class第848行
public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
	if (shutdown || message == null) {
		return;
	}

	if (!job.equals(message.getJob())) {
		throw new IllegalArgumentException("Received DeclineCheckpoint message for job " +
			message.getJob() + " from " + taskManagerLocationInfo + " while this coordinator handles job " + job);
	}

	final long checkpointId = message.getCheckpointId();
	final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");

	PendingCheckpoint checkpoint;

	synchronized (lock) {
		// we need to check inside the lock for being shutdown as well, otherwise we
		// get races and invalid error log messages
		if (shutdown) {
			return;
		}

		checkpoint = pendingCheckpoints.get(checkpointId);

		if (checkpoint != null) {
			Preconditions.checkState(
				!checkpoint.isDiscarded(),
				"Received message for discarded but non-removed checkpoint " + checkpointId);
			LOG.info("Decline checkpoint {} by task {} of job {} at {}.",
				checkpointId,
				message.getTaskExecutionId(),
				job,
				taskManagerLocationInfo);
			final CheckpointException checkpointException;
			if (message.getReason() == null) {
				checkpointException =
					new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED);
			} else {
				checkpointException = getCheckpointException(
					CheckpointFailureReason.JOB_FAILURE, message.getReason());
			}
			abortPendingCheckpoint(
				checkpoint,
				checkpointException,
				message.getTaskExecutionId());
		} else if (LOG.isDebugEnabled()) {
			if (recentPendingCheckpoints.contains(checkpointId)) {
				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				LOG.debug("Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
						checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
			} else {
				// message is for an unknown checkpoint. might be so old that we don't even remember it any more
				LOG.debug("Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
						checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
			}
		}
	}
}

// CheckpointCoordinator.class第1606行
private void abortPendingCheckpoint(
	PendingCheckpoint pendingCheckpoint,
	CheckpointException exception,
	@Nullable final ExecutionAttemptID executionAttemptID) {

	assert(Thread.holdsLock(lock));

	if (!pendingCheckpoint.isDiscarded()) {
		try {
			// release resource here
			pendingCheckpoint.abort(
				exception.getCheckpointFailureReason(), exception.getCause());

			if (pendingCheckpoint.getProps().isSavepoint() &&
				pendingCheckpoint.getProps().isSynchronous()) {
				failureManager.handleSynchronousSavepointFailure(exception);
			} else if (executionAttemptID != null) {
				failureManager.handleTaskLevelCheckpointException(
					exception, pendingCheckpoint.getCheckpointId(), executionAttemptID);
			} else {
				failureManager.handleJobLevelCheckpointException(
					exception, pendingCheckpoint.getCheckpointId());
			}
		} finally {
			sendAbortedMessages(pendingCheckpoint.getCheckpointId(), pendingCheckpoint.getCheckpointTimestamp());
			pendingCheckpoints.remove(pendingCheckpoint.getCheckpointId());
			rememberRecentCheckpointId(pendingCheckpoint.getCheckpointId());
			timer.execute(this::executeQueuedRequest);
		}
	}
}
```
</details>

在收到拒绝信息后，```CheckpointCoordinator```中止```pendingCheckpoint```，通过```sendAbortedMessages```向所有算子发出中止通知（算子接收到后通过Abort on Notification流程中止checkpointing），然后在timer启动一个新的checkpointing。

## Unaligned Barrier（施工中）

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [流式计算系统系列（1）：恰好一次处理](https://zhuanlan.zhihu.com/p/102607983)