# State(5): Checkpointing(下)
2020/11/21

前两篇[State(3): Checkpointing(上)](/engineering/flink/state3.md)和[State(4): Checkpointing(下)](/engineering/flink/state5.md)中介绍了checkpoint的启动和Barrier的处理。本篇是State系列中关于checkpoint的最后一篇，将介绍checkpoint过程中剩余的两个流程：快照的执行过程以及算子完成checkpointing后向```CheckpointCoordinator``` ack的过程。最后将简单汇总Flink1.11版本新加入的Unaligned Barrier机制在每个checkpoint步骤中与aligned Barrier的不同之处。

注：源代码为Flink1.11.0版本

## 执行与中止checkpoint

在前一篇State(3)中提到，算子的checkpointing由```StreamTask#performCheckpoint```方法开始，其中source算子由```StreamTask#triggerCheckpoint```触发，其他算子通过```CheckpointBarrierHandle#notifyCheckpoint```调用```StreamTask#triggerCheckpointOnBarrier```触发。而算子的checkpoint中止则是交由```SubtaskCheckpointCoordinatorImpl#abortCheckpointOnBarrier```执行。

### 算子Checkpointing

```java
private boolean performCheckpoint(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		CheckpointMetrics checkpointMetrics,
		boolean advanceToEndOfTime) throws Exception {

	LOG.debug("Starting checkpoint ({}) {} on task {}",
		checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

	if (isRunning) {
		actionExecutor.runThrowing(() -> {

			if (checkpointOptions.getCheckpointType().isSynchronous()) {
				setSynchronousSavepointId(checkpointMetaData.getCheckpointId());

				if (advanceToEndOfTime) {
					advanceToEndOfEventTime();
				}
			}

			subtaskCheckpointCoordinator.checkpointState(
				checkpointMetaData,
				checkpointOptions,
				checkpointMetrics,
				operatorChain,
				this::isCanceled);
		});

		return true;
	} else {
		actionExecutor.runThrowing(() -> {
			// we cannot perform our checkpoint - let the downstream operators know that they
			// should not wait for any input from this operator

			// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
			// yet be created
			final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
			recordWriter.broadcastEvent(message);
		});

		return false;
	}
}
```

#### 拒绝checkpoint

### 中止checkpoint

## 完成checkpoint

## Unaligned Barrier

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [流式计算系统系列（1）：恰好一次处理](https://zhuanlan.zhihu.com/p/102607983)