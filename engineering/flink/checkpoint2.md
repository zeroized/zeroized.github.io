# Checkpoint(2): 处理Barrier
2020/11/17

前一篇[Checkpoint(1): 启动Checkpoint](/engineering/flink/checkpoint1.md)介绍了checkpointing是如何开始的，包括```CheckpointCoordinator```启动checkpointing和```SubtaskCheckpointCoordinator```向下游发送Barrier。本篇将继续Checkpointing过程的分析，介绍算子收到Barrier后的响应过程。

注：源代码为Flink1.11.0版本

## Barrier与Alignment

算子收到Barrier并开始处理的流程可以认为是从```CheckpointedInputGate#pollNext```开始的：

```java
// CheckpointedInputGate.class第77行
public Optional<BufferOrEvent> pollNext() throws Exception {
	while (true) {
		Optional<BufferOrEvent> next = inputGate.pollNext();

		if (!next.isPresent()) {
			return handleEmptyBuffer();
		}

		BufferOrEvent bufferOrEvent = next.get();
		checkState(!barrierHandler.isBlocked(bufferOrEvent.getChannelInfo()));

		if (bufferOrEvent.isBuffer()) {
			return next;
		}
		else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
			CheckpointBarrier checkpointBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();
			barrierHandler.processBarrier(checkpointBarrier, bufferOrEvent.getChannelInfo());
			return next;
		}
		else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
			barrierHandler.processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent());
		}
		else {
			if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
				barrierHandler.processEndOfPartition();
			}
			return next;
		}
	}
}
```

```pollNext```方法总共会收到4种不同的数据：```Buffer```，一般指StreamElement的物理形式；```CancelCheckpointMarker```，在前一篇的发出Barrier的流程中第一步发出的取消上一checkpoint的标记；```CheckpointBarrier```，即启动算子checkpointing的Barrier；```EndOfPartitionEvent```，一个标识subpartition全部结束的标记。

后3种数据会交由```barrierHandler```处理，即```CheckpointBarrier```的实际处理部分。```CheckpointBarrierHandler```共有4个实现，```分别是CheckpointBarrierTracker```、```CheckpointBarrierAligner```、```CheckpointBarrierUnaligner```和```AlternatingCheckpointBarrierHandler```。这些Handler的实现专注于处理Barrier，通知算子checkpointing、abort等过程位于抽象类中。

<details>
<summary>CheckpointBarrierHandler</summary>

```java
// CheckpointBarrierHandler.class
public abstract class CheckpointBarrierHandler implements Closeable {

	/** The listener to be notified on complete checkpoints. */
	private final AbstractInvokable toNotifyOnCheckpoint;

	private long latestCheckpointStartDelayNanos;

	public CheckpointBarrierHandler(AbstractInvokable toNotifyOnCheckpoint) {
		this.toNotifyOnCheckpoint = checkNotNull(toNotifyOnCheckpoint);
	}

	public void releaseBlocksAndResetBarriers() throws IOException {
	}

	/**
	 * Checks whether the channel with the given index is blocked.
	 *
	 * @param channelInfo The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	public boolean isBlocked(InputChannelInfo channelInfo) {
		return false;
	}

	@Override
	public void close() throws IOException {
	}

	public abstract void processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo) throws Exception;

	public abstract void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception;

	public abstract void processEndOfPartition() throws Exception;

	public abstract long getLatestCheckpointId();

	public long getAlignmentDurationNanos() {
		return 0;
	}

	public long getCheckpointStartDelayNanos() {
		return latestCheckpointStartDelayNanos;
	}

	public Optional<BufferReceivedListener> getBufferReceivedListener() {
		return Optional.empty();
	}

	/**
	 * Returns true if there is in-flight data in the buffers for the given channel and checkpoint. More specifically,
	 * this method returns true iff the unaligner still expects the respective barrier to be <i>consumed</i> on the
	 * that channel.
	 */
	public boolean hasInflightData(long checkpointId, InputChannelInfo channelInfo) {
		return false;
	}

	public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
		return CompletableFuture.completedFuture(null);
	}

	protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier, long alignmentDurationNanos) throws IOException {
		CheckpointMetaData checkpointMetaData =
			new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

		CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
			.setAlignmentDurationNanos(alignmentDurationNanos)
			.setCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);

		toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
			checkpointMetaData,
			checkpointBarrier.getCheckpointOptions(),
			checkpointMetrics);
	}

	protected void notifyAbortOnCancellationBarrier(long checkpointId) throws IOException {
		notifyAbort(checkpointId,
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
	}

	protected void notifyAbort(long checkpointId, CheckpointException cause) throws IOException {
		toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
	}

	protected void markCheckpointStart(long checkpointCreationTimestamp) {
		latestCheckpointStartDelayNanos = 1_000_000 * Math.max(
			0,
			System.currentTimeMillis() - checkpointCreationTimestamp);
	}

	protected <E extends Exception> void executeInTaskThread(
			ThrowingRunnable<E> runnable,
			String descriptionFormat,
			Object... descriptionArgs) throws E {
		toNotifyOnCheckpoint.executeInTaskThread(runnable, descriptionFormat, descriptionArgs);
	}

	protected abstract boolean isCheckpointPending();

	protected void abortPendingCheckpoint(long checkpointId, CheckpointException exception) throws IOException {
	}
}
```
</details>

在通知算子checkpointing（```notifyCheckpoint```方法）后，会调用```StreamTask#triggerCheckpointOnBarrier```方法，然后执行```StreamTask#performCheckpoint```流程（与前一篇的插入Barrier节中的调用栈倒数第二层开始一致，按照5步流程向下游发出Barrier、快照算子自己并通知```CheckpointCoordinator```）。
在通知中止checkpoint时，调用```SubtaskCheckpointCoordinator#abortCheckpointOnBarrier```方法通知```CheckpointCoordinator```取消checkpointing，并向下游发出取消checkpoint的标记```CancelCheckpointMarker```。

在不同的语义级别和不同的checkpoint设置下，在```CheckpointedInputGate```中会使用不同的```CheckpointBarrierHandler```，具体使用哪一个由```InputProcessorUtils#createCheckpointBarrierHandle```决定：

```java
// InputProcessorUtils.class第98行
private static CheckpointBarrierHandler createCheckpointBarrierHandler(
		StreamConfig config,
		InputGate[] inputGates,
		SubtaskCheckpointCoordinator checkpointCoordinator,
		String taskName,
		AbstractInvokable toNotifyOnCheckpoint) {
	switch (config.getCheckpointMode()) {
		case EXACTLY_ONCE:
			if (config.isUnalignedCheckpointsEnabled()) {
				return new AlternatingCheckpointBarrierHandler(
					new CheckpointBarrierAligner(taskName, toNotifyOnCheckpoint, inputGates),
					new CheckpointBarrierUnaligner(checkpointCoordinator, taskName, toNotifyOnCheckpoint, inputGates),
					toNotifyOnCheckpoint);
			}
			return new CheckpointBarrierAligner(taskName, toNotifyOnCheckpoint, inputGates);
		case AT_LEAST_ONCE:
			if (config.isUnalignedCheckpointsEnabled()) {
				throw new IllegalStateException("Cannot use unaligned checkpoints with AT_LEAST_ONCE " +
					"checkpointing mode");
			}
			int numInputChannels = Arrays.stream(inputGates).mapToInt(InputGate::getNumberOfInputChannels).sum();
			return new CheckpointBarrierTracker(numInputChannels, toNotifyOnCheckpoint);
		default:
			throw new UnsupportedOperationException("Unrecognized Checkpointing Mode: " + config.getCheckpointMode());
	}
}
```

可以看到，在```AT_LEAST_ONCE```语义下，Barrier由```CheckpointBarrierTracker```处理；在EXACTLY_ONCE语义下，Aligned Barrier由```CheckpointBarrierAligner```处理，Unaligned Barrier由```AlternatingCheckpointBarrierHandler```，其中同时包含```CheckpointBarrierAligner```和```CheckpointBarrierUnaligner```，当Barrier对应的是一般的checkpoint时（由定时任务定期触发），使用```CheckpointBarrierUnaligner```处理；当Barrier对应的是savepoint时，使用```CheckpointBarrierAligner```处理。下面我们逐一介绍上面提到的三种Barrier处理器。

## CheckpointBarrierTracker

```CheckpointBarrierTracker```仅追踪Barrier并触发算子checkpointing，不会阻塞收到Barrier的通道（即上游算子partition/subpartition），因此只能保证AT_LEAST_ONCE语义。在源代码的类注释中对这部分进行了简介：

>  The {@link CheckpointBarrierTracker} keeps track of what checkpoint barriers have been received from which input channels. Once it has observed all checkpoint barriers for a checkpoint ID, it notifies its listener of a completed checkpoint.
>
> Unlike the {@link CheckpointBarrierAligner}, the BarrierTracker does not block the input channels that have sent barriers, so it cannot be used to gain "exactly-once" processing guarantees. It can, however, be used to gain "at least once" processing guarantees.
>
> NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.

### 处理CheckpointBarrier

```CheckpointBarrierTracker```仅仅是追踪收到的Barrier通道，通过假设到来的checkpoint总是具有更大的id（因为不会阻塞通道，因此先发出的checkpoint在理论上总是更早到），它可以不用判断checkpoint的id而直接进行处理，其处理过程也仅是触发算子checkpointing和计数。由于```CheckpointBarrierTracker```不会阻塞收到Barrier的通道，在```processBarrier```方法中并没有对```InputChannelInfo```进行任何操作，只是简单的记录了一个日志（在debug模式下）：

```java
// CheckpointBarrierTracker.class第79行
public void processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo) throws Exception {
	final long barrierId = receivedBarrier.getId();

	// fast path for single channel trackers
	if (totalNumberOfInputChannels == 1) {
		notifyCheckpoint(receivedBarrier, 0);
		return;
	}

	// general path for multiple input channels
	if (LOG.isDebugEnabled()) {
		LOG.debug("Received barrier for checkpoint {} from channel {}", barrierId, channelInfo);
	}

	// find the checkpoint barrier in the queue of pending barriers
	CheckpointBarrierCount barrierCount = null;
	int pos = 0;

	for (CheckpointBarrierCount next : pendingCheckpoints) {
		if (next.checkpointId == barrierId) {
			barrierCount = next;
			break;
		}
		pos++;
	}

	if (barrierCount != null) {
		// add one to the count to that barrier and check for completion
		int numBarriersNew = barrierCount.incrementBarrierCount();
		if (numBarriersNew == totalNumberOfInputChannels) {
			// checkpoint can be triggered (or is aborted and all barriers have been seen)
			// first, remove this checkpoint and all all prior pending
			// checkpoints (which are now subsumed)
			for (int i = 0; i <= pos; i++) {
				pendingCheckpoints.pollFirst();
			}

			// notify the listener
			if (!barrierCount.isAborted()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received all barriers for checkpoint {}", barrierId);
				}

				notifyCheckpoint(receivedBarrier, 0);
			}
		}
	}
	else {
		// first barrier for that checkpoint ID
		// add it only if it is newer than the latest checkpoint.
		// if it is not newer than the latest checkpoint ID, then there cannot be a
		// successful checkpoint for that ID anyways
		if (barrierId > latestPendingCheckpointID) {
			markCheckpointStart(receivedBarrier.getTimestamp());
			latestPendingCheckpointID = barrierId;
			pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));

			// make sure we do not track too many checkpoints
			if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
				pendingCheckpoints.pollFirst();
			}
		}
	}
}
```

在单通道的情况下，```CheckpointBarrierTracker```直接通知算子启动checkpointing。

在多通道的情况下，```CheckpointBarrierTracker```会维护一个名为```pendingCheckpoints```、正向按照checkpoint id升序的双向队列，其元素保存了checkpoint id和该checkpoint id已到达的Barriers的计数器。当一个Barrier到达时，根据Barrier的id去```pendingCheckpoints```中寻找对应的checkpoint并记录其序号。根据找到与否进行后续的操作：
- 如果找到对应的checkpoint，将其计数器加一。如果此时计数器达到了最大通道数，即所有通道的Barrier都已经到达（在不发生错误的理想情况下），将```pendingCheckpoints```队列中排在该checkpoint前的全部中止，然后触发算子checkpointing（如果这个checkpoint没有被标记为已中止）。
- 如果没有找到对应的checkpoint，首先判断该checkpoint是否比队列最后一个checkpoint晚，满足条件则往队列最后添加该checkpoint（如果队列满了则移除队头）；如果不满足则说明这个checkpoint是异常情况，无视这个checkpoint不做任何处理。

简单总结一下，整个checkpoint能够不出现任何差错需要的前提包括以下3点：
1. 一个通道中不能有重复的Barrier（重复的Barrier会导致计数器会比所有通道的Barrier都到达之前触发）
2. 早发出的Barrier总是更早到（如果后发出的Barrier2早于Barrier1到达算子，checkpoint1就会被无视）
3. checkpointing的到达速度不能过快（过快会导致新的checkpoint频繁“挤掉”最早的checkpoint，使不同通道间允许的延迟时间缩短，增加触发checkpointing的难度）

### 处理CancellationBarrier

在处理中止checkpoint的Barrier时，```CheckpointBarrierTracker```依旧以所有Barrier按照顺序到达为前提：

```java
// CheckpointBarrierTracker.class第145行
public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
	final long checkpointId = cancelBarrier.getCheckpointId();

	if (LOG.isDebugEnabled()) {
		LOG.debug("Received cancellation barrier for checkpoint {}", checkpointId);
	}

	// fast path for single channel trackers
	if (totalNumberOfInputChannels == 1) {
		notifyAbortOnCancellationBarrier(checkpointId);
		return;
	}

	// -- general path for multiple input channels --

	// find the checkpoint barrier in the queue of pending barriers
	// while doing this we "abort" all checkpoints before that one
	CheckpointBarrierCount cbc;
	while ((cbc = pendingCheckpoints.peekFirst()) != null && cbc.checkpointId() < checkpointId) {
		pendingCheckpoints.removeFirst();

		if (cbc.markAborted()) {
			// abort the subsumed checkpoints if not already done
			notifyAbortOnCancellationBarrier(cbc.checkpointId());
		}
	}

	if (cbc != null && cbc.checkpointId() == checkpointId) {
		// make sure the checkpoint is remembered as aborted
		if (cbc.markAborted()) {
			// this was the first time the checkpoint was aborted - notify
			notifyAbortOnCancellationBarrier(checkpointId);
		}

		// we still count the barriers to be able to remove the entry once all barriers have been seen
		if (cbc.incrementBarrierCount() == totalNumberOfInputChannels) {
			// we can remove this entry
			pendingCheckpoints.removeFirst();
		}
	}
	else if (checkpointId > latestPendingCheckpointID) {
		notifyAbortOnCancellationBarrier(checkpointId);

		latestPendingCheckpointID = checkpointId;

		CheckpointBarrierCount abortedMarker = new CheckpointBarrierCount(checkpointId);
		abortedMarker.markAborted();
		pendingCheckpoints.addFirst(abortedMarker);

		// we have removed all other pending checkpoint barrier counts --> no need to check that
		// we don't exceed the maximum checkpoints to track
	} else {
		// trailing cancellation barrier which was already cancelled
	}
}
```

在单通道的情况下，直接触发通知算子中止checkpoint。

在多通道的情况下，```CheckpointBarrierTracker```将正向遍历checkpoint等待队列，逐项移除等待的checkpoint直到找到真正待中止的那一个：
- 如果找到了对应的checkpoint，将其标记为```aborted```状态并执行中止。为了保证确实中止了该checkpoint，这个操作会执行两遍。此时这个checkpoint不会从队列中移除，直到该checkpoint对应的所有Barrier都到达为止才会被移除。
- 如果没有找到对应的checkpoint，且待取消的checkpoint id比原先队尾的更大，直接执行中止，然后将待中止的checkpoint标记为```aborted```然后放到```pendingCheckpoints```队头（此时队列已经被清空，加入后队列中只有这一个元素）；如果checkpoint id比原先队尾的小，则说明该checkpoint已经被中止了，不需要进行任何处理。

## CheckpointBarrierAligner

```CheckpointBarrierAligner```是最基本的Barrier对齐器，要求多通道中所有的通道都收到同一个checkpoint对应的Barrier才会触发算子checkpointing，否则会阻塞已收到Barrier的通道（使其只缓存收到的数据元素而不进行消费）。源代码中的类注释对其介绍如下：

> {@link CheckpointBarrierAligner} keep tracks of received {@link CheckpointBarrier} on given channels and controls the alignment, by deciding which channels should be blocked and when to release blocked channels.

### 处理CheckpointBarrier

由于```CheckpointBarrierAligner```会阻塞通道，因此其处理Barrier的逻辑相对```CheckpointBarrierTracker```更复杂，需要维护所有输入通道的状态，控制通道的阻塞和释放：

```java
// CheckpointBarrierAligner.class第128行
public void processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo) throws Exception {
	final long barrierId = receivedBarrier.getId();

	// fast path for single channel cases
	if (totalNumberOfInputChannels == 1) {
		resumeConsumption(channelInfo);
		if (barrierId > currentCheckpointId) {
			// new checkpoint
			currentCheckpointId = barrierId;
			notifyCheckpoint(receivedBarrier, latestAlignmentDurationNanos);
		}
		return;
	}

	// -- general code path for multiple input channels --

	if (isCheckpointPending()) {
		// this is only true if some alignment is already progress and was not canceled

		if (barrierId == currentCheckpointId) {
			// regular case
			onBarrier(channelInfo);
		}
		else if (barrierId > currentCheckpointId) {
			// we did not complete the current checkpoint, another started before
			LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
					"Skipping current checkpoint.",
				taskName,
				barrierId,
				currentCheckpointId);

			// let the task know we are not completing this
			notifyAbort(currentCheckpointId,
				new CheckpointException(
					"Barrier id: " + barrierId,
					CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED));

			// abort the current checkpoint
			releaseBlocksAndResetBarriers();

			// begin a new checkpoint
			beginNewAlignment(barrierId, channelInfo, receivedBarrier.getTimestamp());
		}
		else {
			// ignore trailing barrier from an earlier checkpoint (obsolete now)
			resumeConsumption(channelInfo);
		}
	}
	else if (barrierId > currentCheckpointId) {
		// first barrier of a new checkpoint
		beginNewAlignment(barrierId, channelInfo, receivedBarrier.getTimestamp());
	}
	else {
		// either the current checkpoint was canceled (numBarriers == 0) or
		// this barrier is from an old subsumed checkpoint
		resumeConsumption(channelInfo);
	}

	// check if we have all barriers - since canceled checkpoints always have zero barriers
	// this can only happen on a non canceled checkpoint
	if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
		// actually trigger checkpoint
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Received all barriers, triggering checkpoint {} at {}.",
				taskName,
				receivedBarrier.getId(),
				receivedBarrier.getTimestamp());
		}

		releaseBlocksAndResetBarriers();
		notifyCheckpoint(receivedBarrier, latestAlignmentDurationNanos);
	}
}

protected void beginNewAlignment(
		long checkpointId,
		InputChannelInfo channelInfo,
		long checkpointTimestamp) throws IOException {
	markCheckpointStart(checkpointTimestamp);
	currentCheckpointId = checkpointId;
	onBarrier(channelInfo);

	startOfAlignmentTimestamp = System.nanoTime();

	if (LOG.isDebugEnabled()) {
		LOG.debug("{}: Starting stream alignment for checkpoint {}.", taskName, checkpointId);
	}
}

/**
 * Blocks the given channel index, from which a barrier has been received.
 *
 * @param channelInfo The channel to block.
 */
protected void onBarrier(InputChannelInfo channelInfo) throws IOException {
	if (!blockedChannels.get(channelInfo)) {
		blockedChannels.put(channelInfo, true);

		numBarriersReceived++;

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Received barrier from channel {}.", taskName, channelInfo);
		}
	}
	else {
		throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelInfo);
	}
}
```

在单通道的情况下，```CheckpointBarrierAligner```首先通知通道继续消费输入元素，然后判断到来的Barrier是否对应一个更新的checkpoint，如果是则触发算子checkpointing，否则无视这个Barrier。在单通道情况下```CheckpointBarrierAligner```不会阻塞通道（也没有必要阻塞通道）。

在多通道的情况下，```CheckpointBarrierAligner```首先判断是否正在进行一个checkpoint（即等待多个通道中的Barrier对齐）并进行相应操作：
- 如果正在进行一个checkpoint，且到来的Barrier就是该checkpoint的Barrier，阻塞该通道并将对齐计数器+1（如果收到了已经阻塞通道的Barrier，抛出一个重复Barrier的异常）；如果到来的Barrier id大于正在进行的checkpoint，通知算子中止checkpointing，并释放所有已阻塞的通道，然后启动一个新的checkpoint（重置checkpoint的时间和id、阻塞该Barrier对应的通道）；如果到来的Barrier id小于正在进行的checkpoint，无视这个Barrier并继续消费通道。
- 如果没有正在进行的checkpoint，且到来的Barrier id比上一次的checkpoint id大，则启动一个新的checkpoint；如果比上一次的checkpoint id小，无视这个Barrier并继续消费通道。

在完成Barrier的处理后，```CheckpointBarrierAligner```判断所有通道的Barrier是否都已经对齐（收到的Barrier数量+已关闭的通道数量=总通道数），如果所有的Barrier都已经对齐，释放所有已阻塞的通道然后触发算子checkpointing。

### 处理CancellationBarrier

```CheckpointBarrierAligner```在处理中止checkpoint的Barrier时，思路和CheckpointBarrier基本是一致的，但无论是否正在进行checkpoint，实际的过程都差不太多：

```java
// CheckpointBarrierAligner.class第238行
public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
	final long barrierId = cancelBarrier.getCheckpointId();

	// fast path for single channel cases
	if (totalNumberOfInputChannels == 1) {
		if (barrierId > currentCheckpointId) {
			// new checkpoint
			currentCheckpointId = barrierId;
			notifyAbortOnCancellationBarrier(barrierId);
		}
		return;
	}

	// -- general code path for multiple input channels --

	if (isCheckpointPending()) {
		// this is only true if some alignment is in progress and nothing was canceled

		if (barrierId == currentCheckpointId) {
			// cancel this alignment
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Checkpoint {} canceled, aborting alignment.", taskName, barrierId);
			}

			releaseBlocksAndResetBarriers();
			notifyAbortOnCancellationBarrier(barrierId);
		}
		else if (barrierId > currentCheckpointId) {
			// we canceled the next which also cancels the current
			LOG.warn("{}: Received cancellation barrier for checkpoint {} before completing current checkpoint {}. " +
					"Skipping current checkpoint.",
				taskName,
				barrierId,
				currentCheckpointId);

			// this stops the current alignment
			releaseBlocksAndResetBarriers();

			// the next checkpoint starts as canceled
			currentCheckpointId = barrierId;
			startOfAlignmentTimestamp = 0L;
			latestAlignmentDurationNanos = 0L;

			notifyAbortOnCancellationBarrier(barrierId);
		}

		// else: ignore trailing (cancellation) barrier from an earlier checkpoint (obsolete now)

	}
	else if (barrierId > currentCheckpointId) {
		// first barrier of a new checkpoint is directly a cancellation

		// by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
		// at zero means that no checkpoint barrier can start a new alignment
		currentCheckpointId = barrierId;

		startOfAlignmentTimestamp = 0L;
		latestAlignmentDurationNanos = 0L;

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Checkpoint {} canceled, skipping alignment.", taskName, barrierId);
		}

		notifyAbortOnCancellationBarrier(barrierId);
	}

	// else: trailing barrier from either
	//   - a previous (subsumed) checkpoint
	//   - the current checkpoint if it was already canceled
}
```

在单通道的情况下，```CheckpointBarrierAligner```直接通知算子中止checkpoint然后将checkpoint id记录下来（如果到来的```CancellationBarrier``` id比当前记录的checkpoint id更大）。

在多通道的情况下，```CheckpointBarrierAligner```同样会根据是否正在进行checkpoint触发两种处理流程：
- 正在进行checkpoint时，如果到来的Barrier就是正在对齐的checkpoint，直接释放所有阻塞的通道，然后通知算子中止checkpoint；如果到来的Barrier id大于正在对齐的checkpoint，同样释放所有阻塞的通道，将当前checkpoint id设置为待中止的id，然后通知算子中止checkpoint。
- 没有进行checkpoint时，如果到来的Barrier id大于正在对齐的checkpoint，将当前checkpoint id设置为待中止的id，然后通知算子中止checkpoint；否则无视这个Barrier。

## CheckpointBarrierUnaligner（施工中）

```CheckpointBarrierUnaligner```在每个checkpoint的第一个Barrier到达时就触发算子checkpointing，并持续追踪后续的Barriers和Buffer。源代码中的类注释如下：

> {@link CheckpointBarrierUnaligner} is used for triggering checkpoint while reading the first barrier and keeping track of the number of received barriers and consumed barriers.

### ThreadSafeUnaligner


## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [流式计算系统系列（1）：恰好一次处理](https://zhuanlan.zhihu.com/p/102607983)