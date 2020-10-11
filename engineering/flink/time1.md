# Time & Watermark(1): Flink中Watermark的生成
2020/10/09

## 相关概念

### Event Time, Processing Time & Ingestion Time

在流式计算概念中，时间可以分为事件时间（Event Time）和处理时间（Processing Time），在这两者的基础上又扩展了摄入时间（Ingestion Time）概念。下面我们展开来说这三种不同的时间概念。

#### Processing Time

> Processing time refers to the system time of the machine that is executing the respective operation. [1]

处理时间是机器对事件执行某种计算操作时的系统时间。Processing Time是事件无关的，不论事件本身是否带有时间戳信息或是事件是否因某些原因延迟到达。当我们设定系统在10:00整触发一次窗口计算后，无论9:59的数据是否到达了系统，到10:00的瞬间系统就会开始计算，而迟到的数据则会被放到下一个计算窗口中进行。

我们显然能够发现，使用Processing Time会导致以下两个问题：
1. 使用Processing Time丢失了数据本身自带的时间信息，导致与数据发生时间相关的计算无法进行（无法准确进行）
2. 由于系统在计算时只计算已到达的数据，当需要进行重新计算时（尤其是因容错导致的重算），难以保证两次计算的结果是一致的（不可重复）

#### Event Time

> Event time is the time that each individual event occurred on its producing device. [1]

事件时间是每个事件个体发生时的事件。根据Event Time的计算规则，当我们设定系统计算10:00前的窗口时，系统应该等待所有的数据都到达系统后再触发计算。这样就解决了上面Processing Time带来的两个问题。但这只是一种理想状态，在分布式系统中，下游系统永远无法知道10:00前到底会来多少条数据，是不是还有因延迟没有到达的数据；在实际生产环境中，也不可能一直等下去。

虽然相比Processing Time，Event Time在处理时间相关数据时具有极大的优势，但其同样带来了性能上的劣势：
1. 使用Event Time需要为每一条数据进行标记，当数据的时间戳信息是需要转换的时候，会产生大量的性能损耗
2. 在允许lateness的场景下，乱序数据频繁的延迟到达会反复触发重算，占用额外的计算资源

#### Ingestion Time

摄入时间是指事件/数据进入系统的时间，其本质上是处理时间的变体。使用Ingestion Time能够避免在数据计算链路上的ETL过程产生的延迟（导致经过多次计算后发生乱序），保证先到的事件/数据无论在传播过程中发生怎样的延迟，总能在逻辑意义上早于晚进入系统的事件/数据。这种意义上，Ingestion Time又更像是（先验地认为数据进入系统是有序的）Event Time。如果事件/数据一经产生就发送到系统中，或是事件本身不具有时间戳信息但计算过程对顺序有要求，那么Ingestion Time几乎可以等同于Event Time[4]。

### Watermark

在Processing Time场景下，系统可以根据内置时钟，使用定时触发器来固定间隔触发计算；而在Event Time场景下，由于事件/数据的Event Time与系统时间无关，需要一种能够度量Event Time场景下事件/数据流的进度。Watermark正是这样一种用于度量Event Time进度的机制。

> A watermark is a notion of input completeness with respect to event times. A watermark with a value of time X makes the statement: “all input data with event times less than X have been observed.” [3]
>
> A Watermark(t) declares that event time has reached time t in that stream, meaning that there should be no more elements from the stream with a timestamp t’ <= t (i.e. events with timestamps older or equal to the watermark). [1]

Watermark是用于在Event Time场景下标识输入完成的标志，表明“Event Time在X以前的所有数据都已经被观测到了”。值得注意的是，Watermark本身并没有改变下游系统“永远无法预知上游事件/数据是否都已经到达”这一事实，而是针对“已经被下游系统观察到”的事件/数据进行的先验性假设。当事件/数据进入计算系统后，由接收的部分（Flink中是DataStreamSource）负责对数据流添加Watermark（Flink使用的是向数据流中插入Watermark）。数据流在计算系统的算子间流动时，由于不同的计算时延、不同的窗口划分会不可避免地发生乱序（或是在接收到数据时就已经是乱序），那么当算子读到某个Watermark(t)时，该算子就知道t时间以前所有的数据都已经到达，可以进行窗口聚合计算了。

根据Watermark对完整性的保证类型不同，Watermark又分为Perfect Watermark和Heuristic Watermark[2]。Perfect Watermark准确地描述了输入流的完整性，在实际生产环境下由于各个算子的计算延迟和分区容错，Perfect Watermark几乎不可能达到。Heuristic Watermark则是对输入流完整性的一种启发式猜测，通常根据输入的特性进行启发式规则的设置，如分区、分区内顺序、文件增长速度等等[2]。通过启发式规则生成的Watermark过慢会导致计算结果延迟过久；而过快则会导致经常丢失数据，计算结果准确率下降。

## Flink中的Watermark

### Watermark的生成

> Watermarks are generated at, or directly after, source functions. [1]

Flink中Watermark的生成分为两种：
1. 直接在Source中生成，对应代码中```SourceContext```接口的实现```WatermarkContext```抽象类（其子类```ManualWatermarkContext```类和```AutomaticWatermarkContext```类分别对应Event Time和Ingestion Time）
2. 在Source后生成，对应代码中的DataStream类中```assignTimestampsAndWatermarks(WatermarkStrategy<T>)```方法

#### 在Source中生成Watermark

我们首先来看一下生成和启动Source算子的流程中与生成Watermark相关的部分。

```java
// StreamSource.class 第90行
// run(final Object,final StreamStatusMaintainer,final Output<StreamRecord<OUT>>,final OperatorChain<?, ?>) throws Exception
this.ctx = StreamSourceContexts.getSourceContext(
			timeCharacteristic,
			getProcessingTimeService(),
			lockingObject,
			streamStatusMaintainer,
			collector,
			watermarkInterval,
			-1);

// StreamSourceContexts.class 第56行
// getSourceContext(TimeCharacteristic,ProcessingTimeService,Object,StreamStatusMaintainer,Output<StreamRecord<OUT>>,long,long)
switch (timeCharacteristic) {
	case EventTime:
		ctx = new ManualWatermarkContext<>(
			output,
			processingTimeService,
			checkpointLock,
			streamStatusMaintainer,
			idleTimeout);
		break;
	case IngestionTime:
		ctx = new AutomaticWatermarkContext<>(
			output,
			watermarkInterval,
			processingTimeService,
			checkpointLock,
			streamStatusMaintainer,
			idleTimeout);
		break;
	case ProcessingTime:
		ctx = new NonTimestampContext<>(checkpointLock, output);
		break;
	default:
		throw new IllegalArgumentException(String.valueOf(timeCharacteristic));
}
```

很容易看到，根据环境中配置的TimeCharacteristic场景不同，Flink会给Source提供不同的上下文。```SourceContext```接口包含6个接口方法，其中```collect(T)```（包含自动生成Watermark的逻辑）、```collectWithTimestamp(T,long)```（直接赋予Watermark）和```emitWatermark(Watermark)```两个方法与Watermark直接相关，```markAsTemporarilyIdle()```方法与Watermark间接相关。

<details>
<summary>SourceContext接口</summary>

```java
// SourceFunction.class第182行
/**
 * Interface that source functions use to emit elements, and possibly watermarks.
 *
 * @param <T> The type of the elements produced by the source.
 */
@Public // Interface might be extended in the future with additional methods.
interface SourceContext<T> {

	/**
	 * Emits one element from the source, without attaching a timestamp. In most cases,
	 * this is the default way of emitting elements.
	 *
	 * <p>The timestamp that the element will get assigned depends on the time characteristic of
	 * the streaming program:
	 * <ul>
	 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the element has no timestamp.</li>
	 *     <li>On {@link TimeCharacteristic#IngestionTime}, the element gets the system's
	 *         current time as the timestamp.</li>
	 *     <li>On {@link TimeCharacteristic#EventTime}, the element will have no timestamp initially.
	 *         It needs to get a timestamp (via a {@link TimestampAssigner}) before any time-dependent
	 *         operation (like time windows).</li>
	 * </ul>
	 *
	 * @param element The element to emit
	 */
	void collect(T element);

	/**
	 * Emits one element from the source, and attaches the given timestamp. This method
	 * is relevant for programs using {@link TimeCharacteristic#EventTime}, where the
	 * sources assign timestamps themselves, rather than relying on a {@link TimestampAssigner}
	 * on the stream.
	 *
	 * <p>On certain time characteristics, this timestamp may be ignored or overwritten.
	 * This allows programs to switch between the different time characteristics and behaviors
	 * without changing the code of the source functions.
	 * <ul>
	 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the timestamp will be ignored,
	 *         because processing time never works with element timestamps.</li>
	 *     <li>On {@link TimeCharacteristic#IngestionTime}, the timestamp is overwritten with the
	 *         system's current time, to realize proper ingestion time semantics.</li>
	 *     <li>On {@link TimeCharacteristic#EventTime}, the timestamp will be used.</li>
	 * </ul>
	 *
	 * @param element The element to emit
	 * @param timestamp The timestamp in milliseconds since the Epoch
	 */
	@PublicEvolving
	void collectWithTimestamp(T element, long timestamp);

	/**
	 * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
	 * elements with a timestamp {@code t' <= t} will occur any more. If further such
	 * elements will be emitted, those elements are considered <i>late</i>.
	 *
	 * <p>This method is only relevant when running on {@link TimeCharacteristic#EventTime}.
	 * On {@link TimeCharacteristic#ProcessingTime},Watermarks will be ignored. On
	 * {@link TimeCharacteristic#IngestionTime}, the Watermarks will be replaced by the
	 * automatic ingestion time watermarks.
	 *
	 * @param mark The Watermark to emit
	 */
	@PublicEvolving
	void emitWatermark(Watermark mark);

	/**
	 * Marks the source to be temporarily idle. This tells the system that this source will
	 * temporarily stop emitting records and watermarks for an indefinite amount of time. This
	 * is only relevant when running on {@link TimeCharacteristic#IngestionTime} and
	 * {@link TimeCharacteristic#EventTime}, allowing downstream tasks to advance their
	 * watermarks without the need to wait for watermarks from this source while it is idle.
	 *
	 * <p>Source functions should make a best effort to call this method as soon as they
	 * acknowledge themselves to be idle. The system will consider the source to resume activity
	 * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T, long)},
	 * or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or watermarks from the source.
	 */
	@PublicEvolving
	void markAsTemporarilyIdle();

	/**
	 * Returns the checkpoint lock. Please refer to the class-level comment in
	 * {@link SourceFunction} for details about how to write a consistent checkpointed
	 * source.
	 *
	 * @return The object to use as the lock
	 */
	Object getCheckpointLock();

	/**
	 * This method is called by the system to shut down the context.
	 */
	void close();
}
```
</details>

虽然Processing Time场景与Watermark无关，但因为和Event Time、Ingestion Time放在了同一个```switch```分支语句中，于是这里就顺带介绍一下```NonTimestampContext```：

<details>
<summary>NonTimestampContext类</summary>

```java
// StreamSourceContexts.class第89行
/**
 * A source context that attached {@code -1} as a timestamp to all records, and that
 * does not forward watermarks.
 */
private static class NonTimestampContext<T> implements SourceFunction.SourceContext<T> {

	private final Object lock;
	private final Output<StreamRecord<T>> output;
	private final StreamRecord<T> reuse;

	private NonTimestampContext(Object checkpointLock, Output<StreamRecord<T>> output) {
		this.lock = Preconditions.checkNotNull(checkpointLock, "The checkpoint lock cannot be null.");
		this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
		this.reuse = new StreamRecord<>(null);
	}

	@Override
	public void collect(T element) {
		synchronized (lock) {
			output.collect(reuse.replace(element));
		}
	}

	@Override
	public void collectWithTimestamp(T element, long timestamp) {
		// ignore the timestamp
		collect(element);
	}

	@Override
	public void emitWatermark(Watermark mark) {
		// do nothing
	}

	@Override
	public void markAsTemporarilyIdle() {
		// do nothing
	}

	@Override
	public Object getCheckpointLock() {
		return lock;
	}

	@Override
	public void close() {}
}
```
	
</details>

果然相当直接地跳过了```emitWatermark```的部分。

让我们回到正题。Event Time和Ingestion Time的Watermark上下文部分都继承了```WatermarkContext```抽象类：

<details>
<summary>WatermarkContext抽象类</summary>

```java
// StreamSourceContexts.class第339行
/**
 * An abstract {@link SourceFunction.SourceContext} that should be used as the base for
 * stream source contexts that are relevant with {@link Watermark}s.
 *
 * <p>Stream source contexts that are relevant with watermarks are responsible of manipulating
 * the current {@link StreamStatus}, so that stream status can be correctly propagated
 * downstream. Please refer to the class-level documentation of {@link StreamStatus} for
 * information on how stream status affects watermark advancement at downstream tasks.
 *
 * <p>This class implements the logic of idleness detection. It fires idleness detection
 * tasks at a given interval; if no records or watermarks were collected by the source context
 * between 2 consecutive checks, it determines the source to be IDLE and correspondingly
 * toggles the status. ACTIVE status resumes as soon as some record or watermark is collected
 * again.
 */
private abstract static class WatermarkContext<T> implements SourceFunction.SourceContext<T> {

	protected final ProcessingTimeService timeService;
	protected final Object checkpointLock;
	protected final StreamStatusMaintainer streamStatusMaintainer;
	protected final long idleTimeout;

	private ScheduledFuture<?> nextCheck;

	/**
	 * This flag will be reset to {@code true} every time the next check is scheduled.
	 * Whenever a record or watermark is collected, the flag will be set to {@code false}.
	 *
	 * <p>When the scheduled check is fired, if the flag remains to be {@code true}, the check
	 * will fail, and our current status will determined to be IDLE.
	 */
	private volatile boolean failOnNextCheck;

	/**
	 * Create a watermark context.
	 *
	 * @param timeService the time service to schedule idleness detection tasks
	 * @param checkpointLock the checkpoint lock
	 * @param streamStatusMaintainer the stream status maintainer to toggle and retrieve current status
	 * @param idleTimeout (-1 if idleness checking is disabled)
	 */
	public WatermarkContext(
			final ProcessingTimeService timeService,
			final Object checkpointLock,
			final StreamStatusMaintainer streamStatusMaintainer,
			final long idleTimeout) {

		this.timeService = Preconditions.checkNotNull(timeService, "Time Service cannot be null.");
		this.checkpointLock = Preconditions.checkNotNull(checkpointLock, "Checkpoint Lock cannot be null.");
		this.streamStatusMaintainer = Preconditions.checkNotNull(streamStatusMaintainer, "Stream Status Maintainer cannot be null.");

		if (idleTimeout != -1) {
			Preconditions.checkArgument(idleTimeout >= 1, "The idle timeout cannot be smaller than 1 ms.");
		}
		this.idleTimeout = idleTimeout;

		scheduleNextIdleDetectionTask();
	}

	@Override
	public void collect(T element) {
		synchronized (checkpointLock) {
			streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

			if (nextCheck != null) {
				this.failOnNextCheck = false;
			} else {
				scheduleNextIdleDetectionTask();
			}

			processAndCollect(element);
		}
	}

	@Override
	public void collectWithTimestamp(T element, long timestamp) {
		synchronized (checkpointLock) {
			streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

			if (nextCheck != null) {
				this.failOnNextCheck = false;
			} else {
				scheduleNextIdleDetectionTask();
			}

			processAndCollectWithTimestamp(element, timestamp);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		if (allowWatermark(mark)) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					scheduleNextIdleDetectionTask();
				}

				processAndEmitWatermark(mark);
			}
		}
	}

	@Override
	public void markAsTemporarilyIdle() {
		synchronized (checkpointLock) {
			streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
		}
	}

	@Override
	public Object getCheckpointLock() {
		return checkpointLock;
	}

	@Override
	public void close() {
		cancelNextIdleDetectionTask();
	}

	private class IdlenessDetectionTask implements ProcessingTimeCallback {
		@Override
		public void onProcessingTime(long timestamp) throws Exception {
			synchronized (checkpointLock) {
				// set this to null now;
				// the next idleness detection will be scheduled again
				// depending on the below failOnNextCheck condition
				nextCheck = null;

				if (failOnNextCheck) {
					markAsTemporarilyIdle();
				} else {
					scheduleNextIdleDetectionTask();
				}
			}
		}
	}

	private void scheduleNextIdleDetectionTask() {
		if (idleTimeout != -1) {
			// reset flag; if it remains true when task fires, we have detected idleness
			failOnNextCheck = true;
			nextCheck = this.timeService.registerTimer(
				this.timeService.getCurrentProcessingTime() + idleTimeout,
				new IdlenessDetectionTask());
		}
	}

	protected void cancelNextIdleDetectionTask() {
		final ScheduledFuture<?> nextCheck = this.nextCheck;
		if (nextCheck != null) {
			nextCheck.cancel(true);
		}
	}

	// ------------------------------------------------------------------------
	//	Abstract methods for concrete subclasses to implement.
	//  These methods are guaranteed to be synchronized on the checkpoint lock,
	//  so implementations don't need to do so.
	// ------------------------------------------------------------------------

	/** Process and collect record. */
	protected abstract void processAndCollect(T element);

	/** Process and collect record with timestamp. */
	protected abstract void processAndCollectWithTimestamp(T element, long timestamp);

	/** Whether or not a watermark should be allowed. */
	protected abstract boolean allowWatermark(Watermark mark);

	/**
	 * Process and emit watermark. Only called if
	 * {@link WatermarkContext#allowWatermark(Watermark)} returns {@code true}.
	 */
	protected abstract void processAndEmitWatermark(Watermark mark);

}
```
</details>

```WatermarkContext```抽象类对SourceContext的6个方法都进行了实现或部分实现（主要是加锁、状态切换等，与Watermark的生成关系不大），可以看到具体的Watermark生成逻辑位于```processAndCollect(T)```和```processAndEmitWatermark(Watermark)```方法的实现，并通过```allowWatermark(Watermark)```方法判断Watermark的合理性。

在Event Time场景下，Watermark由数据本身决定，因此其对应的上下文```ManualWatermarkContext```没有对Watermark的时间戳进行任何处理，```allowWatermark(Watermark)```直接返回了```true```:

<details>
<summary>ManualWatermarkContext类</summary>

```java
// StreamSourceContexts.class第285行
/**
 * A SourceContext for event time. Sources may directly attach timestamps and generate
 * watermarks, but if records are emitted without timestamps, no timestamps are automatically
 * generated and attached. The records will simply have no timestamp in that case.
 *
 * <p>Streaming topologies can use timestamp assigner functions to override the timestamps
 * assigned here.
 */
private static class ManualWatermarkContext<T> extends WatermarkContext<T> {

	private final Output<StreamRecord<T>> output;
	private final StreamRecord<T> reuse;

	private ManualWatermarkContext(
			final Output<StreamRecord<T>> output,
			final ProcessingTimeService timeService,
			final Object checkpointLock,
			final StreamStatusMaintainer streamStatusMaintainer,
			final long idleTimeout) {

		super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

		this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
		this.reuse = new StreamRecord<>(null);
	}

	@Override
	protected void processAndCollect(T element) {
		output.collect(reuse.replace(element));
	}

	@Override
	protected void processAndCollectWithTimestamp(T element, long timestamp) {
		output.collect(reuse.replace(element, timestamp));
	}

	@Override
	protected void processAndEmitWatermark(Watermark mark) {
		output.emitWatermark(mark);
	}

	@Override
	protected boolean allowWatermark(Watermark mark) {
		return true;
	}
}
```
</details>

而在Ingestion Time场景下，Watermark是自动生成的，因此其对应的上下文```AutomaticWatermarkContext```就比```ManualWatermarkContext```要复杂不少：

<details>
<summary>AutomaticWatermarkContext类</summary>

```java
// StreamSourceContexts.class第137行
/**
 * {@link SourceFunction.SourceContext} to be used for sources with automatic timestamps
 * and watermark emission.
 */
private static class AutomaticWatermarkContext<T> extends WatermarkContext<T> {

	private final Output<StreamRecord<T>> output;
	private final StreamRecord<T> reuse;

	private final long watermarkInterval;

	private volatile ScheduledFuture<?> nextWatermarkTimer;
	private volatile long nextWatermarkTime;

	private long lastRecordTime;

	private AutomaticWatermarkContext(
			final Output<StreamRecord<T>> output,
			final long watermarkInterval,
			final ProcessingTimeService timeService,
			final Object checkpointLock,
			final StreamStatusMaintainer streamStatusMaintainer,
			final long idleTimeout) {

		super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

		this.output = Preconditions.checkNotNull(output, "The output cannot be null.");

		Preconditions.checkArgument(watermarkInterval >= 1L, "The watermark interval cannot be smaller than 1 ms.");
		this.watermarkInterval = watermarkInterval;

		this.reuse = new StreamRecord<>(null);

		this.lastRecordTime = Long.MIN_VALUE;

		long now = this.timeService.getCurrentProcessingTime();
		this.nextWatermarkTimer = this.timeService.registerTimer(now + watermarkInterval,
			new WatermarkEmittingTask(this.timeService, checkpointLock, output));
	}

	@Override
	protected void processAndCollect(T element) {
		lastRecordTime = this.timeService.getCurrentProcessingTime();
		output.collect(reuse.replace(element, lastRecordTime));

		// this is to avoid lock contention in the lockingObject by
		// sending the watermark before the firing of the watermark
		// emission task.
		if (lastRecordTime > nextWatermarkTime) {
			// in case we jumped some watermarks, recompute the next watermark time
			final long watermarkTime = lastRecordTime - (lastRecordTime % watermarkInterval);
			nextWatermarkTime = watermarkTime + watermarkInterval;
			output.emitWatermark(new Watermark(watermarkTime));

			// we do not need to register another timer here
			// because the emitting task will do so.
		}
	}

	@Override
	protected void processAndCollectWithTimestamp(T element, long timestamp) {
		processAndCollect(element);
	}

	@Override
	protected boolean allowWatermark(Watermark mark) {
		// allow Long.MAX_VALUE since this is the special end-watermark that for example the Kafka source emits
		return mark.getTimestamp() == Long.MAX_VALUE && nextWatermarkTime != Long.MAX_VALUE;
	}

	/** This will only be called if allowWatermark returned {@code true}. */
	@Override
	protected void processAndEmitWatermark(Watermark mark) {
		nextWatermarkTime = Long.MAX_VALUE;
		output.emitWatermark(mark);

		// we can shutdown the watermark timer now, no watermarks will be needed any more.
		// Note that this procedure actually doesn't need to be synchronized with the lock,
		// but since it's only a one-time thing, doesn't hurt either
		final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
		if (nextWatermarkTimer != null) {
			nextWatermarkTimer.cancel(true);
		}
	}

	@Override
	public void close() {
		super.close();

		final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
		if (nextWatermarkTimer != null) {
			nextWatermarkTimer.cancel(true);
		}
	}

	private class WatermarkEmittingTask implements ProcessingTimeCallback {

		private final ProcessingTimeService timeService;
		private final Object lock;
		private final Output<StreamRecord<T>> output;

		private WatermarkEmittingTask(
				ProcessingTimeService timeService,
				Object checkpointLock,
				Output<StreamRecord<T>> output) {
			this.timeService = timeService;
			this.lock = checkpointLock;
			this.output = output;
		}

		@Override
		public void onProcessingTime(long timestamp) {
			final long currentTime = timeService.getCurrentProcessingTime();

			synchronized (lock) {
				// we should continue to automatically emit watermarks if we are active
				if (streamStatusMaintainer.getStreamStatus().isActive()) {
					if (idleTimeout != -1 && currentTime - lastRecordTime > idleTimeout) {
						// if we are configured to detect idleness, piggy-back the idle detection check on the
						// watermark interval, so that we may possibly discover idle sources faster before waiting
						// for the next idle check to fire
						markAsTemporarilyIdle();

						// no need to finish the next check, as we are now idle.
						cancelNextIdleDetectionTask();
					} else if (currentTime > nextWatermarkTime) {
						// align the watermarks across all machines. this will ensure that we
						// don't have watermarks that creep along at different intervals because
						// the machine clocks are out of sync
						final long watermarkTime = currentTime - (currentTime % watermarkInterval);

						output.emitWatermark(new Watermark(watermarkTime));
						nextWatermarkTime = watermarkTime + watermarkInterval;
					}
				}
			}

			long nextWatermark = currentTime + watermarkInterval;
			nextWatermarkTimer = this.timeService.registerTimer(
					nextWatermark, new WatermarkEmittingTask(this.timeService, lock, output));
		}
	}
}
```
</details>

```AutomaticWatermarkContext```的机制有几个关键点：
1. ```processAndCollectWithTimestamp(T, long)```方法直接调用了```processAndCollect(T)```进行处理，直接忽略了直接赋予的Watermark
2. 使用定时任务```WatermarkEmittingTask```来保证所有的机器上的nextWatermarkTime一定会更新到watermarkInterval的整数倍（同时生成上一个整数倍位置的Watermark），避免机器时钟不同步导致的Watermark不同步
3. 由2可知，Ingestion Time场景下的Watermark一定是watermarkInterval的整数倍（间隔通过env在job启动前设置）
4. 在```processAndCollect(T)```方法中，为了避免锁争用导致的```WatermarkEmittingTask```Watermark同步错误，因此每次收集新记录时（只要满足同步条件）就会手动进行一次nextWatermarkTime同步
5. 当且仅当参数Watermark为```Long.MAX_VALUE```时（诸如Kafka等Source的结束特殊标识）时，才允许手动调用```emitWatermark(Watermark)```方法，此时Source会认为输入已经结束

#### 使用WatermarkStrategy生成Watermark

使用WatermarkStrategy生成，即调用DataStream类实例对象的```assignTimestampsAndWatermarks(WatermarkStrategy<T>)```方法生成Watermark。使用这种方式生成Watermark并不限于Source算子，而是可以对任意一个算子操作（其本质是对算子进行了一次transformation，对算子产出的StreamRecord进行Watermark和Timestamp的后处理）。需要注意的是，在1.11版本中，原来的```AssignerWithPunctuatedWatermarks<T>```或```AssignerWithPeriodicWatermarks<T>```已经被标记为过期方法，不建议继续使用，Punctuated（对应```onEvent(T,long,WatermarkOutput)```）和Periodic（对应```onPeriodicEmit(WatermarkOutput)```）生成方法被合并到了WatermarkStrategy接口中。关于WatermarkStrategy如何实现，在[Flink官方文档](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/event_timestamps_watermarks.html)中已经介绍的很详细了，这里就不再赘述了。

```java
// DataStream.class第897行
public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(
		WatermarkStrategy<T> watermarkStrategy) {

	final WatermarkStrategy<T> cleanedStrategy = clean(watermarkStrategy);

	final TimestampsAndWatermarksOperator<T> operator =
		new TimestampsAndWatermarksOperator<>(cleanedStrategy);

	// match parallelism to input, to have a 1:1 source -> timestamps/watermarks relationship and chain
	final int inputParallelism = getTransformation().getParallelism();

	return transform("Timestamps/Watermarks", getTransformation().getOutputType(), operator)
		.setParallelism(inputParallelism);
}
```

此处生成了一个```TimestampsAndWatermarksOperator```作为处理Watermark和事件/数据Timestamp的算子（Transform），然后将原先定义的算子和Watermark算子合并成为一个算子。

<details>
<summary>TimestampsAndWatermarksOperator</summary>

```java
// TimestampsAndWatermarksOperator.class
/**
 * A stream operator that may do one or both of the following: extract timestamps from
 * events and generate watermarks.
 *
 * <p>These two responsibilities run in the same operator rather than in two different ones,
 * because the implementation of the timestamp assigner and the watermark generator is
 * frequently in the same class (and should be run in the same instance), even though the
 * separate interfaces support the use of different classes.
 *
 * @param <T> The type of the input elements
 */
public class TimestampsAndWatermarksOperator<T>
		extends AbstractStreamOperator<T>
		implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private final WatermarkStrategy<T> watermarkStrategy;

	/** The timestamp assigner. */
	private transient TimestampAssigner<T> timestampAssigner;

	/** The watermark generator, initialized during runtime. */
	private transient WatermarkGenerator<T> watermarkGenerator;

	/** The watermark output gateway, initialized during runtime. */
	private transient WatermarkOutput wmOutput;

	/** The interval (in milliseconds) for periodic watermark probes. Initialized during runtime. */
	private transient long watermarkInterval;

	public TimestampsAndWatermarksOperator(
			WatermarkStrategy<T> watermarkStrategy) {

		this.watermarkStrategy = checkNotNull(watermarkStrategy);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		timestampAssigner = watermarkStrategy.createTimestampAssigner(this::getMetricGroup);
		watermarkGenerator = watermarkStrategy.createWatermarkGenerator(this::getMetricGroup);

		wmOutput = new WatermarkEmitter(output, getContainingTask().getStreamStatusMaintainer());

		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0) {
			final long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
	}

	@Override
	public void processElement(final StreamRecord<T> element) throws Exception {
		final T event = element.getValue();
		final long previousTimestamp = element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE;
		final long newTimestamp = timestampAssigner.extractTimestamp(event, previousTimestamp);

		element.setTimestamp(newTimestamp);
		output.collect(element);
		watermarkGenerator.onEvent(event, newTimestamp, wmOutput);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		watermarkGenerator.onPeriodicEmit(wmOutput);

		final long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream, except for the "end of time" watermark.
	 */
	@Override
	public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE) {
			wmOutput.emitWatermark(Watermark.MAX_WATERMARK);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		watermarkGenerator.onPeriodicEmit(wmOutput);
	}

	// ------------------------------------------------------------------------

	/**
	 * Implementation of the {@code WatermarkEmitter}, based on the components
	 * that are available inside a stream operator.
	 */
	private static final class WatermarkEmitter implements WatermarkOutput {

		private final Output<?> output;

		private final StreamStatusMaintainer statusMaintainer;

		private long currentWatermark;

		private boolean idle;

		WatermarkEmitter(Output<?> output, StreamStatusMaintainer statusMaintainer) {
			this.output = output;
			this.statusMaintainer = statusMaintainer;
			this.currentWatermark = Long.MIN_VALUE;
		}

		@Override
		public void emitWatermark(Watermark watermark) {
			final long ts = watermark.getTimestamp();

			if (ts <= currentWatermark) {
				return;
			}

			currentWatermark = ts;

			if (idle) {
				idle = false;
				statusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
			}

			output.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(ts));
		}

		@Override
		public void markIdle() {
			idle = true;
			statusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
		}
	}
}
```
</details>

在```TimestampsAndWatermarksOperator```中，每次处理事件都会调用一次```WatermarkGenerator```接口实现的```onEvent(T,long,WatermarkOutput)```方法，因此官方指南中给出的Punctuated Watermark的使用方法是对该方法进行实现。

而```WatermarkGenerator```接口中的```onPeriodicEmit(WatermarkOutput)```方法在```onProcessingTime(long)```方法中被调用。```onProcessingTime(long)```实现自```ProcessingTimeCallback```接口，当定时任务被触发时就会执行。因此，```onPeriodicEmit(WatermarkOutput)```方法会周期性地执行并生成Watermark，官方指南给出实现该方法作为使用Periodical Watermark的方法。

除了上述两个对Watermark具体生成时间进行控制的部分，```TimestampsAndWatermarksOperator```中还有两个关键点（这里实际上属于后一篇——Watermark的传播与处理——中的内容）：
1. ```TimestampsAndWatermarksOperator```覆写了其父类```AbstractStreamOperator```中```processWatermark(Watermark)```的过程，因此一旦一个算子使用assignTimestampsAndWatermarks(WatermarkStrategy<T>)进行水印的添加，它将不再处理来自上游的Watermark并按照自己设定的逻辑生成并向下游发送新的Watermark（除非遇到和前面所说一致的流结束标识Watermark）。
2. 使用```WatermarkEmitter```作为```WatermarkOutput```接口的实现，在传播Watermark时，比较当前待发出的Watermark与已发出所有Watermark中最晚的时间戳```currentWatermark```，如果当前带发出Watermark比```currentWatermark```早，则会被丢弃（在语义上形成了冗余）。

## 参考文献

1. [Timely Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/timely-stream-processing.html)
2. [Streaming 102: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
3. [流式计算系统系列（2）：时间](https://zhuanlan.zhihu.com/p/103472646)