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

另一方面，StreamStatus的变化也会导致Valve输出Watermark。当收到的StreamStatus将一个ACTIVE的Channel状态修改为IDLE：
    - 如果此时所有的Channel都变为闲置状态，且该Channel输出了```lastOutputWatermark```，则执行FlushAll操作，输出所有Channel状态中时间戳最大的Watermark，以触发下游算子可能的最晚的计算
    - 如果此时还有其他激活状态的Channel，且该Channel输出了```lastOutputWatermark```，则执行输出Watermark流程的第3步，发送同步Channel中最早的Watermark

## 参考文献

1. [Timely Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/timely-stream-processing.html)
2. [Streaming 102: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
3. [流式计算系统系列（2）：时间](https://zhuanlan.zhihu.com/p/103472646)