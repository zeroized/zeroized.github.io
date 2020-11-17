# State(4): Checkpointing(下)
2020/11/17

前一篇[State(3): Checkpointing(上)](/engineering/flink/state3.md)介绍了checkpointing是如何开始的，包括```CheckpointCoordinator```启动checkpointing和```SubtaskCheckpointCoordinator```向下游发送Barrier。本篇将继续Checkpointing过程的分析，介绍算子收到Barrier后的响应、快照的执行过程以及算子完成checkpointing后向```CheckpointCoordinator``` ack的过程。最后将简单介绍Flink1.11版本新加入的Unaligned Barrier机制的实现。

注：源代码为Flink1.11.0版本

## Barrier与Alignment

算子收到Barrier并开始处理的流程可以认为是从```CheckpointedInputGate#pollNext```开始的：

```java
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
在通知丢弃checkpoint时，调用```SubtaskCheckpointCoordinator#abortCheckpointOnBarrier```方法通知```CheckpointCoordinator```取消checkpointing，并向下游发出取消checkpoint的标记```CancelCheckpointMarker```。

在不同的语义级别和不同的checkpoint设置下，在```CheckpointedInputGate```中会使用不同的```CheckpointBarrierHandler```，具体使用哪一个由```InputProcessorUtils#createCheckpointBarrierHandle```决定：

```java
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

可以看到，在```AT_LEAST_ONCE```语义下，Barrier由```CheckpointBarrierTracker```处理；在EXACTLY_ONCE语义下，Aligned Barrier由```CheckpointBarrierAligner```处理，Unaligned Barrier由```AlternatingCheckpointBarrierHandler```，其中同时包含```CheckpointBarrierAligner```和```CheckpointBarrierUnaligner```。下面我们逐一介绍这三种情况。

### CheckpointBarrierTracker

>  The {@link CheckpointBarrierTracker} keeps track of what checkpoint barriers have been received from which input channels. Once it has observed all checkpoint barriers for a checkpoint ID, it notifies its listener of a completed checkpoint.
>
> Unlike the {@link CheckpointBarrierAligner}, the BarrierTracker does not block the input channels that have sent barriers, so it cannot be used to gain "exactly-once" processing guarantees. It can, however, be used to gain "at least once" processing guarantees.
>
> NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.

<details>
<summary>CheckpointBarrierTracker</summary>

```java
public class CheckpointBarrierTracker extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierTracker.class);

	/**
	 * The tracker tracks a maximum number of checkpoints, for which some, but not all barriers
	 * have yet arrived.
	 */
	private static final int MAX_CHECKPOINTS_TO_TRACK = 50;

	// ------------------------------------------------------------------------

	/**
	 * The number of channels. Once that many barriers have been received for a checkpoint, the
	 * checkpoint is considered complete.
	 */
	private final int totalNumberOfInputChannels;

	/**
	 * All checkpoints for which some (but not all) barriers have been received, and that are not
	 * yet known to be subsumed by newer checkpoints.
	 */
	private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;

	/** The highest checkpoint ID encountered so far. */
	private long latestPendingCheckpointID = -1;

	public CheckpointBarrierTracker(int totalNumberOfInputChannels, AbstractInvokable toNotifyOnCheckpoint) {
		super(toNotifyOnCheckpoint);
		this.totalNumberOfInputChannels = totalNumberOfInputChannels;
		this.pendingCheckpoints = new ArrayDeque<>();
	}

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

	@Override
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

	@Override
	public void processEndOfPartition() throws Exception {
		while (!pendingCheckpoints.isEmpty()) {
			CheckpointBarrierCount barrierCount = pendingCheckpoints.removeFirst();
			if (barrierCount.markAborted()) {
				notifyAbort(barrierCount.checkpointId(),
					new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
			}
		}
	}

	public long getLatestCheckpointId() {
		return pendingCheckpoints.isEmpty() ? -1 : pendingCheckpoints.peekLast().checkpointId();
	}

	public boolean isCheckpointPending() {
		return !pendingCheckpoints.isEmpty();
	}

	/**
	 * Simple class for a checkpoint ID with a barrier counter.
	 */
	private static final class CheckpointBarrierCount {

		private final long checkpointId;

		private int barrierCount;

		private boolean aborted;

		CheckpointBarrierCount(long checkpointId) {
			this.checkpointId = checkpointId;
			this.barrierCount = 1;
		}

		public long checkpointId() {
			return checkpointId;
		}

		public int incrementBarrierCount() {
			return ++barrierCount;
		}

		public boolean isAborted() {
			return aborted;
		}

		public boolean markAborted() {
			boolean firstAbort = !this.aborted;
			this.aborted = true;
			return firstAbort;
		}

		@Override
		public String toString() {
			return isAborted() ?
				String.format("checkpointID=%d - ABORTED", checkpointId) :
				String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
		}
	}
}
```
</details>

## Snapshotting

## 完成checkpoint

## Unaligned Barrier

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [流式计算系统系列（1）：恰好一次处理](https://zhuanlan.zhihu.com/p/102607983)