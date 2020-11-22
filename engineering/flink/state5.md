# State(5): Checkpointing(下)
2020/11/21

前两篇[State(3): Checkpointing(上)](/engineering/flink/state3.md)和[State(4): Checkpointing(下)](/engineering/flink/state5.md)中介绍了checkpoint的启动和Barrier的处理。本篇是State系列中关于checkpoint的最后一篇，将介绍checkpoint过程中剩余的两个流程：快照的执行过程以及算子完成checkpointing后向```CheckpointCoordinator``` ack的过程。最后将简单汇总Flink1.11版本新加入的Unaligned Barrier机制在每个checkpoint步骤中与aligned Barrier的不同之处。

注：源代码为Flink1.11.0版本

## 执行与中止checkpoint

在前一篇State(3)中提到，算子的checkpointing以```StreamTask#performCheckpoint```方法开始，其中包含5个步骤（即State(3)篇中发出Barrier的流程）：
1. 如果上一个checkpoint是失败的checkpoint，向下游算子广播中止上一个checkpoint的事件```CancelCheckpointMarker```
2. 告知Source以及和Source链接在一起的所有算子准备Barrier前的快照
3. 向下游算子广播Barrier事件```CheckpointBarrier```
4. 如果是Unaligned Barrier，对正在发送过程中的数据元素进行快照
5. 调用```takeSnapshotSync```方法对算子状态进行快照，如果在这个步骤发生错误，清理失败的快照并向```channelStateWriter```发出checkpoint失败消息

source算子的```performCheckpoint```方法由```StreamTask#triggerCheckpoint```触发；其他算子通过```CheckpointBarrierHandle#notifyCheckpoint```调用```StreamTask#triggerCheckpointOnBarrier```触发。而算子的checkpoint中止则是交由```SubtaskCheckpointCoordinatorImpl```的```abortCheckpointOnBarrier```方法执行。

### 算子快照（Snapshotting）

在算子checkpointing的最后一步，即在向下游发出Barrier后进行算子的快照（snapshot）。算子快照的起点是```SubtaskCheckpointCoordinatorImpl#takeSnapshotSync```方法：

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

首先协调器会从尾部开始遍历链接的算子，确认链上所有的算子都处于open状态，否则拒绝这个checkpoint（```Environment#declineCheckpoint```）。然后根据checkpoint的设置获取checkpoint写入目标的通道，从链接算子尾部向头部逐个调用```SubtaskCheckpointCoordinatorImpl#buildOperatorSnapshotFutures```方法异步地构造算子快照：

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

在这个过程中，所有的算子都会通过```StreamOperatorStateHandler#snapshotState```方法进行算子中keyed状态和Operator状态的快照。对于头部算子，需要额外向checkpoint目标通道中写入算子输入通道（即接收Buffer和Barrier的通道）的状态；对尾部算子则需要额外写入算子结果子分片（即输出Buffer和Barrier的通道）的状态。

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

算子的快照共包含4个部分：
- 快照时间服务
- 算子自定义快照逻辑（```AbstractStreamOperator#snapshotState```是空实现，各个算子override这个方法实现自己的额外快照逻辑）
- 将算子状态快照并写入后端
- 将keyed状态快照并写入后端

#### 拒绝checkpoint

### Unaligned算子快照（施工中）

### 中止checkpoint

## 完成checkpoint

## Unaligned Barrier

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [流式计算系统系列（1）：恰好一次处理](https://zhuanlan.zhihu.com/p/102607983)