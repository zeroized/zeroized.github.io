# Time & Watermark(2): Watermark的传播与处理
2020/10/12

在上一篇[Time & Watermark(1): Flink中Watermark的生成](/engineering/flink/time1.md)中介绍了三种Time和Watermark的概念，以及这三种Time在Flink中对应的Watermark生成方案。本篇将进一步研究Watermark，分析Watermark是怎么在Flink中发挥作用的。

注：源代码为Flink1.11.0版本

## Watermark的传播

### Watermark的实现

在Flink中，```Watermark```类是数据流中的一类特殊元素，和```StreamRecord```类一样是```StreamElement```的子类，所以不论是数据元素走的```Context#collect```或```Context#collectWithTimestamp```还是Watermark走的```Context#emitWatermark```，本质上都是将它们放进了数据流中。

```java
public final class Watermark extends StreamElement {

	/** The watermark that signifies end-of-event-time. */
	public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

	/** The timestamp of the watermark in milliseconds. */
	private final long timestamp;

    // constructor, getter, equals, hashCode & toString
}
```

### 发出Watermark

在上一篇的```WatermarkContext```中能看到，Watermark是通过```Output#emitWatermark```发出的。```Output```接口有诸多实现，其中```IterationTailOutput```没有实现Watermark的处理（迭代流将下游算子的结果重新指向上游算子的输入，其带有的Watermark无法继续使用）；```CountingOutput```（记录Watermark数量，```AbstractStreamOperator```类默认的output）、```DirectedOutput```（）等等均是```Output```的装饰器用于实现各种指标的度量，其具体实现是```RecordWriterOutput```类。```ChainingOutput```虽然不是一个装饰器，但其本身并不会执行output的逻辑，而是将record依次（反向递归）推给链上的算子处理。

```RecordWriterOutput```实现自```WatermarkGaugeExposingOutput```接口，该接口强化了```Output```，暴露出一个获取当前Watermark进度的方法：

```java
// OperatorChain.class第559行
public interface WatermarkGaugeExposingOutput<T> extends Output<T> {
	Gauge<Long> getWatermarkGauge();
}

// RecordWriterOutput.class第115行
// emitWatermark(Watermark)方法
@Override
public void emitWatermark(Watermark mark) {
	watermarkGauge.setCurrentWatermark(mark.getTimestamp());
	serializationDelegate.setInstance(mark);

	if (streamStatusProvider.getStreamStatus().isActive()) {
		try {
			recordWriter.broadcastEmit(serializationDelegate);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}
```

可以看到在发出Watermark的时候进行了两步操作：
1. 将```Output```的Watermark进度更新为即将发出的Watermark时间戳
2. 使用广播发送的方式向所有下游算子发出Watermark

### Watermark阀

Output仅负责发出Watermark，而不负责决定一个Watermark能不能向下游传播。当数据流在不断流动时，Watermark基本不会对算子产生影响；而当Source进入闲置状态时，Watermark可能会引发一系列混乱（对这些问题的描述原文见```StreamStatus```的类注释）：
1. 算子仅有一个上游算子，该上游算子进入闲置状态，但定时任务依然在继续发出Watermark
2. 算子有多个上游算子，其中的某个上游算子进入闲置状态/从闲置状态中恢复但Watermark大幅落后于其他上游算子

<details>
<summary>原文</summary>

Stream Status elements received at downstream tasks also affect and control how their operators process and advance their watermarks. The below describes the effects (the logic is implemented as a {@link StatusWatermarkValve} which downstream tasks should use for such purposes):

- Since source tasks guarantee that no records will be emitted between a {@link StreamStatus#IDLE} and {@link StreamStatus#ACTIVE}, downstream tasks can always safely process and propagate records through their operator chain when they receive them, without the need to check whether or not the task is currently idle or active. However, for watermarks, since there may be watermark generators that might produce watermarks anywhere in the middle of topologies regardless of whether there are input data at the operator, the current status of the task must be checked before forwarding watermarks emitted from an operator. If the status is actually idle, the watermark must be blocked.

- For downstream tasks with multiple input streams, the watermarks of input streams that are temporarily idle, or has resumed to be active but its watermark is behind the overall min watermark of the operator, should not be accounted for when deciding whether or not to advance the watermark and propagated through the operator chain.


Note that to notify downstream tasks that a source task is permanently closed and will no longer send any more elements, the source should still send a {@link Watermark#MAX_WATERMARK} instead of {@link StreamStatus#IDLE}. Stream Status elements only serve as markers for temporary status.

</details>

多个上游的情况是相当常见的情况，如```KeyBy```场景，多流```connect```/```union```场景，在这些场景下，使用上游中最小的Watermark从结果上看是一定不会出错的（兜底策略）[3]。但实际生产环境中，一旦发生上述第二种情况，下游算子实际触发计算的Watermark大幅晚于应该的Watermark，导致按时到达的正常输入数据挤压在下游算子中。针对这种情况，需要一个“阀门”来控制Watermark（和StreamStatus），这个“阀门”就是```StatusWatermarkValve```。

<details>
<summary>StatusWatermarkValve</summary>

```java
public class StatusWatermarkValve {

	private final DataOutput output;

	// ------------------------------------------------------------------------
	//	Runtime state for watermark & stream status output determination
	// ------------------------------------------------------------------------

	/**
	 * Array of current status of all input channels. Changes as watermarks & stream statuses are
	 * fed into the valve.
	 */
	private final InputChannelStatus[] channelStatuses;

	/** The last watermark emitted from the valve. */
	private long lastOutputWatermark;

	/** The last stream status emitted from the valve. */
	private StreamStatus lastOutputStreamStatus;

	/**
	 * Returns a new {@code StatusWatermarkValve}.
	 *
	 * @param numInputChannels the number of input channels that this valve will need to handle
	 * @param output the customized output handler for the valve
	 */
	public StatusWatermarkValve(int numInputChannels, DataOutput output) {
		checkArgument(numInputChannels > 0);
		this.channelStatuses = new InputChannelStatus[numInputChannels];
		for (int i = 0; i < numInputChannels; i++) {
			channelStatuses[i] = new InputChannelStatus();
			channelStatuses[i].watermark = Long.MIN_VALUE;
			channelStatuses[i].streamStatus = StreamStatus.ACTIVE;
			channelStatuses[i].isWatermarkAligned = true;
		}

		this.output = checkNotNull(output);

		this.lastOutputWatermark = Long.MIN_VALUE;
		this.lastOutputStreamStatus = StreamStatus.ACTIVE;
	}

	/**
	 * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new Watermark,
	 * {@link DataOutput#emitWatermark(Watermark)} will be called to process the new Watermark.
	 *
	 * @param watermark the watermark to feed to the valve
	 * @param channelIndex the index of the channel that the fed watermark belongs to (index starting from 0)
	 */
	public void inputWatermark(Watermark watermark, int channelIndex) throws Exception {
		// ignore the input watermark if its input channel, or all input channels are idle (i.e. overall the valve is idle).
		if (lastOutputStreamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isActive()) {
			long watermarkMillis = watermark.getTimestamp();

			// if the input watermark's value is less than the last received watermark for its input channel, ignore it also.
			if (watermarkMillis > channelStatuses[channelIndex].watermark) {
				channelStatuses[channelIndex].watermark = watermarkMillis;

				// previously unaligned input channels are now aligned if its watermark has caught up
				if (!channelStatuses[channelIndex].isWatermarkAligned && watermarkMillis >= lastOutputWatermark) {
					channelStatuses[channelIndex].isWatermarkAligned = true;
				}

				// now, attempt to find a new min watermark across all aligned channels
				findAndOutputNewMinWatermarkAcrossAlignedChannels();
			}
		}
	}

	/**
	 * Feed a {@link StreamStatus} into the valve. This may trigger the valve to output either a new Stream Status,
	 * for which {@link DataOutput#emitStreamStatus(StreamStatus)} will be called, or a new Watermark,
	 * for which {@link DataOutput#emitWatermark(Watermark)} will be called.
	 *
	 * @param streamStatus the stream status to feed to the valve
	 * @param channelIndex the index of the channel that the fed stream status belongs to (index starting from 0)
	 */
	public void inputStreamStatus(StreamStatus streamStatus, int channelIndex) throws Exception {
		// only account for stream status inputs that will result in a status change for the input channel
		if (streamStatus.isIdle() && channelStatuses[channelIndex].streamStatus.isActive()) {
			// handle active -> idle toggle for the input channel
			channelStatuses[channelIndex].streamStatus = StreamStatus.IDLE;

			// the channel is now idle, therefore not aligned
			channelStatuses[channelIndex].isWatermarkAligned = false;

			// if all input channels of the valve are now idle, we need to output an idle stream
			// status from the valve (this also marks the valve as idle)
			if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {

				// now that all input channels are idle and no channels will continue to advance its watermark,
				// we should "flush" all watermarks across all channels; effectively, this means emitting
				// the max watermark across all channels as the new watermark. Also, since we already try to advance
				// the min watermark as channels individually become IDLE, here we only need to perform the flush
				// if the watermark of the last active channel that just became idle is the current min watermark.
				if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
					findAndOutputMaxWatermarkAcrossAllChannels();
				}

				lastOutputStreamStatus = StreamStatus.IDLE;
				output.emitStreamStatus(lastOutputStreamStatus);
			} else if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
				// if the watermark of the channel that just became idle equals the last output
				// watermark (the previous overall min watermark), we may be able to find a new
				// min watermark from the remaining aligned channels
				findAndOutputNewMinWatermarkAcrossAlignedChannels();
			}
		} else if (streamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isIdle()) {
			// handle idle -> active toggle for the input channel
			channelStatuses[channelIndex].streamStatus = StreamStatus.ACTIVE;

			// if the last watermark of the input channel, before it was marked idle, is still larger than
			// the overall last output watermark of the valve, then we can set the channel to be aligned already.
			if (channelStatuses[channelIndex].watermark >= lastOutputWatermark) {
				channelStatuses[channelIndex].isWatermarkAligned = true;
			}

			// if the valve was previously marked to be idle, mark it as active and output an active stream
			// status because at least one of the input channels is now active
			if (lastOutputStreamStatus.isIdle()) {
				lastOutputStreamStatus = StreamStatus.ACTIVE;
				output.emitStreamStatus(lastOutputStreamStatus);
			}
		}
	}

	private void findAndOutputNewMinWatermarkAcrossAlignedChannels() throws Exception {
		long newMinWatermark = Long.MAX_VALUE;
		boolean hasAlignedChannels = false;

		// determine new overall watermark by considering only watermark-aligned channels across all channels
		for (InputChannelStatus channelStatus : channelStatuses) {
			if (channelStatus.isWatermarkAligned) {
				hasAlignedChannels = true;
				newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
			}
		}

		// we acknowledge and output the new overall watermark if it really is aggregated
		// from some remaining aligned channel, and is also larger than the last output watermark
		if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
			lastOutputWatermark = newMinWatermark;
			output.emitWatermark(new Watermark(lastOutputWatermark));
		}
	}

	private void findAndOutputMaxWatermarkAcrossAllChannels() throws Exception {
		long maxWatermark = Long.MIN_VALUE;

		for (InputChannelStatus channelStatus : channelStatuses) {
			maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
		}

		if (maxWatermark > lastOutputWatermark) {
			lastOutputWatermark = maxWatermark;
			output.emitWatermark(new Watermark(lastOutputWatermark));
		}
	}

	/**
	 * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
	 * status, and whether or not the channel's current watermark is aligned with the overall
	 * watermark output from the valve.
	 *
	 * <p>There are 2 situations where a channel's watermark is not considered aligned:
	 * <ul>
	 *   <li>the current stream status of the channel is idle
	 *   <li>the stream status has resumed to be active, but the watermark of the channel hasn't
	 *   caught up to the last output watermark from the valve yet.
	 * </ul>
	 */
	@VisibleForTesting
	protected static class InputChannelStatus {
		protected long watermark;
		protected StreamStatus streamStatus;
		protected boolean isWatermarkAligned;

		/**
		 * Utility to check if at least one channel in a given array of input channels is active.
		 */
		private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
			for (InputChannelStatus status : channelStatuses) {
				if (status.streamStatus.isActive()) {
					return true;
				}
			}
			return false;
		}
	}

	@VisibleForTesting
	protected InputChannelStatus getInputChannelStatus(int channelIndex) {
		Preconditions.checkArgument(
			channelIndex >= 0 && channelIndex < channelStatuses.length,
			"Invalid channel index. Number of input channels: " + channelStatuses.length);

		return channelStatuses[channelIndex];
	}
}
```
</details>

```StatusWatermarkValve```的主要工作流程是，接收来自上游的Watermark和StreamStatus，更新接收上游数据的Channel状态，然后决定是否要将接收到的Watermark或StreamStatus输出到下游算子中。在Watermark部分，其具体的工作流程为：
1. 判断新到的Watermark是否是一个有意义的Watermark（其时间戳晚于该Channel先前收到的最晚的Watermark），如果有意义则在Channel状态中更新Watermark状态
2. 判断新到的Watermark时间戳是否比```lastOutputWatermark```时间更晚（即判断是不是上面情况二中延迟的Watermark），如果不是一个延迟Watermark，则将这个Channel标记为同步（aligned）Channel
3. 检查所有同步Channel中的Watermark状态，记录其中最早的Watermark时间戳。如果存在同步Channel，且该最早时间戳比```lastOutputWatermark```晚，向下游输出该时间戳作为Watermark

另一方面，StreamStatus的变化也会导致Valve输出Watermark。当收到的StreamStatus将一个```ACTIVE```的Channel状态修改为```IDLE```：
- 如果此时所有的Channel都变为闲置状态，且该Channel输出了```lastOutputWatermark```，则执行FlushAll操作，输出所有Channel状态中时间戳最大的Watermark，以触发下游算子可能的最晚的计算
- 如果此时还有其他激活状态的Channel，且该Channel输出了```lastOutputWatermark```，则执行输出Watermark流程的第3步，发送同步Channel中最早的Watermark

## Watermark的处理

### 基本处理过程

Flink的流算子分为两类，分别是只有一个输入流、实现```OneInputStreamOperator```接口的单输入流算子，对应的Watermark处理方法是```processWatermark(Watermark)```；以及有两个输入流、实现```TwoInputStreamOperator```接口的双输入流算子，对应的Watermark处理方法是```processWatermark1(Watermark)```和```processWatermark2(Watermark)```。```AbstractStreamOperator```抽象类继承了这两个接口，并为所有的算子提供了最基础的功能实现。值得一提的是，两个输入流的Watermark处理方法是完全一致的，并在后续版本中会使用```AbstractStreamOperatorV2```代替```AbstractStreamOperator```并同时支持多输入流算子```MultipleInputStreamOperator```接口（目前仅在test中有实现）。

```java
// AbstractStreamOperator.class第566行
public void processWatermark(Watermark mark) throws Exception {
	if (timeServiceManager != null) {
		timeServiceManager.advanceWatermark(mark);
	}
	output.emitWatermark(mark);
}

public void processWatermark1(Watermark mark) throws Exception {
	input1Watermark = mark.getTimestamp();
	long newMin = Math.min(input1Watermark, input2Watermark);
	if (newMin > combinedWatermark) {
		combinedWatermark = newMin;
		processWatermark(new Watermark(combinedWatermark));
	}
}

public void processWatermark2(Watermark mark) throws Exception {
	input2Watermark = mark.getTimestamp();
	long newMin = Math.min(input1Watermark, input2Watermark);
	if (newMin > combinedWatermark) {
		combinedWatermark = newMin;
		processWatermark(new Watermark(combinedWatermark));
	}
}

// AbstractStreamOperatorV2.class第473行
protected void reportWatermark(Watermark mark, int inputId) throws Exception {
	inputWatermarks[inputId - 1] = mark.getTimestamp();
	long newMin = mark.getTimestamp();
	for (long inputWatermark : inputWatermarks) {
		newMin = Math.min(inputWatermark, newMin);
	}
	if (newMin > combinedWatermark) {
		combinedWatermark = newMin;
		processWatermark(new Watermark(combinedWatermark));
	}
}
```

对双输入乃至多输入的情况，Base算子对Watermark的处理是一致的，均是取了输入流中最小的Watermark再处理，处理完成以后再向下游发出该Watermark。实际处理Watermark的流程为```AbstractStreamOperator#processWatermark```$\to$```InternalTimeServiceManager#advanceWatermark```$\to$```InternalTimerServiceImpl#advanceWatermark```。

```java
// InternalTimerServiceImpl.class第268行
public void advanceWatermark(long time) throws Exception {
	currentWatermark = time;

	InternalTimer<K, N> timer;

	while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
		eventTimeTimersQueue.poll();
		keyContext.setCurrentKey(timer.getKey());
		triggerTarget.onEventTime(timer);
	}
}
```

基本处理逻辑就是，当一个新Watermark到来时，将Event Time的计时器队列中所有到时间的计时器取出来，然后触发对应的触发器```Triggerable#onEventTime```。单输入流触发器包括与窗口相关的```WindowOperator```及其子类和与窗口无关的```KeyedProcessOperator```；双输入流触发器包括```IntervalJoinOperator```、```KeyedCoProcessOperator```和```CoBroadcastWithKeyedOperator```

### 特殊处理过程

大部分继承```AbstractStreamOperator```的流算子都使用了父类提供的Watermark处理方法，并记录了当前Watermark的状态，形如：
```java
@Override
public void processWatermark(Watermark mark) throws Exception {
	super.processWatermark(mark);
	currentWatermark = mark.getTimestamp();
}
```

一部分算子无视了来自上游的Watermark。这些算子往往自己生成Watermark，比如上一篇中提到的```TimestampsAndWatermarksOperator```、处理```TimestampedFileInputSplit```的```ContinuousFileReaderOperator```

Async I/O算子```AsyncWaitOperator```将Watermark当成一个处理完成的数据元素（见```WatermarkQueueEntry```）放入异步I/O队列中，处理根据异步I/O结果是否有序有不同的处理流程。Watermark的到达一定会触发一次Async I/O的结果输出（调用```StreamElementQueue#emitCompletedElement```方法）。

```java
// AsyncWaitOperator.class第199行
@Override
public void processWatermark(Watermark mark) throws Exception {
	addToWorkQueue(mark);

	// watermarks are always completed
	// if there is no prior element, we can directly emit them
	// this also avoids watermarks being held back until the next element has been processed
	outputCompletedElement();
}

// AsyncWaitOperator.class第254行
private ResultFuture<OUT> addToWorkQueue(StreamElement streamElement) throws InterruptedException {

	Optional<ResultFuture<OUT>> queueEntry;
	while (!(queueEntry = queue.tryPut(streamElement)).isPresent()) {
		mailboxExecutor.yield();
	}

	return queueEntry.get();
}

// AsyncWaitOperator.class第277行 
private void outputCompletedElement() {
	if (queue.hasCompletedElements()) {
		// emit only one element to not block the mailbox thread unnecessarily
		queue.emitCompletedElement(timestampedCollector);
		// if there are more completed elements, emit them with subsequent mails
		if (queue.hasCompletedElements()) {
			mailboxExecutor.execute(this::outputCompletedElement, "AsyncWaitOperator#outputCompletedElement");
		}
	}
}
```

结果有序的情况下，Watermark被直接放入异步I/O处理队列中。

<details>
<summary>结果有序Async I/O算子相关Watermark处理逻辑</summary>

```java
// OrderedStreamElementQueue.class第62行
@Override
public boolean hasCompletedElements() {
	return !queue.isEmpty() && queue.peek().isDone();
}

@Override
public void emitCompletedElement(TimestampedCollector<OUT> output) {
	if (hasCompletedElements()) {
		final StreamElementQueueEntry<OUT> head = queue.poll();
		head.emitResult(output);
	}
}

// OrderedStreamElementQueue.class第95行
@Override
public Optional<ResultFuture<OUT>> tryPut(StreamElement streamElement) {
	if (queue.size() < capacity) {
		StreamElementQueueEntry<OUT> queueEntry = createEntry(streamElement);

		queue.add(queueEntry);

		LOG.debug("Put element into ordered stream element queue. New filling degree " +
			"({}/{}).", queue.size(), capacity);

		return Optional.of(queueEntry);
	} else {
		LOG.debug("Failed to put element into ordered stream element queue because it " +
			"was full ({}/{}).", queue.size(), capacity);

		return Optional.empty();
	}
}

private StreamElementQueueEntry<OUT> createEntry(StreamElement streamElement) {
	if (streamElement.isRecord()) {
		return new StreamRecordQueueEntry<>((StreamRecord<?>) streamElement);
	}
	if (streamElement.isWatermark()) {
		return new WatermarkQueueEntry<>((Watermark) streamElement);
	}
	throw new UnsupportedOperationException("Cannot enqueue " + streamElement);
}
```
</details>

结果无序的情况下，Watermark会把数据元素分割成若干段，只有先前所有的段都已经完成（或超时）并发出后，才能发出下一个段的异步I/O返回数据。Watermark的到来会创建一个容量为1的新段（或是重用前一个已经处理完毕但还没有回收的段），然后在该段后再创建一个正常容量的段。由于Watermark所在的段只包含Watermark一个元素，且Watermark的完成状态永远是true，所以Watermark对应段总能在触发输出时被发出并移出队列。

<details>
<summary>结果无序Aysnc I/O算子相关Watermark处理逻辑</summary>

```java
// UnorderedStreamElementQueue.class第140行
@Override
public boolean hasCompletedElements() {
	return !this.segments.isEmpty() && this.segments.getFirst().hasCompleted();
}

@Override
public void emitCompletedElement(TimestampedCollector<OUT> output) {
	if (segments.isEmpty()) {
		return;
	}
	final Segment currentSegment = segments.getFirst();
	numberOfEntries -= currentSegment.emitCompleted(output);

	// remove any segment if there are further segments, if not leave it as an optimization even if empty
	if (segments.size() > 1 && currentSegment.isEmpty()) {
		segments.pop();
	}
}

// UnorderedStreamElementQueue.class第121行
private StreamElementQueueEntry<OUT> addWatermark(Watermark watermark) {
	Segment<OUT> watermarkSegment;
	if (!segments.isEmpty() && segments.getLast().isEmpty()) {
		// reuse already existing segment if possible (completely drained) or the new segment added at the end of
		// this method for two succeeding watermarks
		watermarkSegment = segments.getLast();
	} else {
		watermarkSegment = addSegment(1);
	}

	StreamElementQueueEntry<OUT> watermarkEntry = new WatermarkQueueEntry<>(watermark);
	watermarkSegment.add(watermarkEntry);

	// add a new segment for actual elements
	addSegment(capacity);
	return watermarkEntry;
}
```
</details>

## 参考文献

1. [Timely Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/timely-stream-processing.html)
2. [Streaming 102: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
3. [流式计算系统系列（2）：时间](https://zhuanlan.zhihu.com/p/103472646)