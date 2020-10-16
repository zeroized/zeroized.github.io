# Window(2): 触发器与回收器
2020/10/15

继[Window(1): 窗口的分配](/engineering/flink/window1.md)中介绍了当一个数据元素进入窗口算子后是如何被分配到具体的窗口中的。在本篇中，将继续介绍触发窗口计算的触发器以及触发器触发后、窗口计算方法执行前/后处理窗口元素的回收器。

## 窗口的触发

### 触发器的实现

窗口触发器共分为两个部分：具体触发逻辑实现（实现```Trigger```接口）和触发器上下文（实现```Trigger.TriggerContext```接口）。

```java
// Trigger.class
public abstract class Trigger<T, W extends Window> implements Serializable {

	private static final long serialVersionUID = -4104633972991191369L;

	public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;

	public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception;

	public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;

	public boolean canMerge() {
		return false;
	}

	public void onMerge(W window, OnMergeContext ctx) throws Exception {
		throw new UnsupportedOperationException("This trigger does not support merging.");
	}

	public abstract void clear(W window, TriggerContext ctx) throws Exception;

	public interface TriggerContext {

		long getCurrentProcessingTime();

		MetricGroup getMetricGroup();

		long getCurrentWatermark();

		void registerProcessingTimeTimer(long time);

		void registerEventTimeTimer(long time);

		void deleteProcessingTimeTimer(long time);

		void deleteEventTimeTimer(long time);

		<S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);
	}

	public interface OnMergeContext extends TriggerContext {
		<S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor);
	}
}
```

```Trigger```属于窗口算子、不属于任何一个窗口，且```TriggerContext```只提供了状态和时间的get方法，其本身也不负责维护窗口的状态，因此在使用触发器时（修改触发器、计算触发结果）需要窗口算子将上下文切换到对应窗口的```namespace```。

在Flink自身实现中与内置的窗口配套的有```EventTimeTrigger```（作为Event Time窗口的默认触发器）、```ProcessingTimeTrigger```（作为Processing Time窗口的默认触发器）、```NeverTrigger```（作为Global窗口的触发器）、```CountTrigger```（作为KeyedStream#countWindow的触发器，countWindow方法实质是全局窗口分配器+计数触发器的组合）以及一个装饰器```PurgingTrigger```（在内嵌触发器触发后返回FIRE_AND_PURGE结果，然后清除窗口中的元素）。其他的内置实现包括```ContinuousEventTimeTrigger```、```ContinuousProcessingTimeTrigger```和```DeltaTrigger```。

触发器的触发结果包括四种情况，分别是```CONTINUE```（没有触发）、```FIRE_AND_PURGE```（触发窗口计算方法然后清除窗口内元素）、```FIRE```（只触发窗口计算方法）和```PURGE```（只清除窗口内元素）：

```java
// TriggerResult.class
public enum TriggerResult {

	CONTINUE(false, false),

	FIRE_AND_PURGE(true, true),

	FIRE(true, false),

	PURGE(false, true);

	// ------------------------------------------------------------------------

	private final boolean fire;
	private final boolean purge;

	// constructor and getters
}
```

### 触发器的触发

在新数据元素进入算子或Processing Time计时器或Watermark进入算子时会计算触发结果，对应```WindowOperator#processElement```、```WindowOperator#onProcessingTime```和```WindowOperator#onEventTime```三个方法。

- ```WindowOperator#onEventTime```方法的触发参考[Time & Watermark(2): Watermark的传播与处理](/engineering/flink/time2.md)篇，Watermark的基本处理流程中的调用链```InternalTimerServiceImpl#advanceWatermark```$\to$```Triggerable#onEventTime```
- ```WindowOperator#onProcessingTime```方法由```InternalTimerServiceImpl#onProcessingTime```$\to$```Triggerable#onProcessingTime```触发，是根据系统时间设定的定时任务

实际Event Time和Processing Time的计算逻辑是完全一致的，因此系统的时间特性几乎是对触发器透明的。

#### 由数据元素触发

在处理数据元素流程中，触发器的计算紧跟在窗口分配和窗口合并后（合并窗口场景下与之基本一致）：

```java
// WindowOperator.class第360行
// processElement(StreamRecord<IN>) throws Exception方法
windowState.setCurrentNamespace(stateWindow);
windowState.add(element.getValue());

triggerContext.key = key;
triggerContext.window = actualWindow;

TriggerResult triggerResult = triggerContext.onElement(element);

if (triggerResult.isFire()) {
	ACC contents = windowState.get();
	if (contents == null) {
		continue;
	}
	emitWindowContents(actualWindow, contents);
}

if (triggerResult.isPurge()) {
	windowState.clear();
}
registerCleanupTimer(actualWindow);
```

当窗口分配和合并完成后，触发器会对元素进行计算，得到一个触发结果。当处罚结果为```CONTINUE```时，不进行任何操作；当触发结果为```FIRE```时，从窗口状态中取出当前窗口的状态，触发执行窗口计算方法，并发出一个结果；当触发结果为```PURGE```时，清空当前窗口的状态；```FIRE_AND_PURGE```则会先后执行```FIRE```和```PURGE```的流程。

#### 由时间触发

时间触发的触发器计算过程基本与数据元素一致，唯一不同的是上下文```namespace```的获取需要通过触发时间方法的```InternalTime```来提供（```onProcessingTime```的过程基本与之相同）：

```java
// WindowOperator.class第447行
@Override
public void onEventTime(InternalTimer<K, W> timer) throws Exception {
	triggerContext.key = timer.getKey();
	triggerContext.window = timer.getNamespace();

	MergingWindowSet<W> mergingWindows;

	if (windowAssigner instanceof MergingWindowAssigner) {
		mergingWindows = getMergingWindowSet();
		W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
		if (stateWindow == null) {
			// Timer firing for non-existent window, this can only happen if a
			// trigger did not clean up timers. We have already cleared the merging
			// window and therefore the Trigger state, however, so nothing to do.
			return;
		} else {
			windowState.setCurrentNamespace(stateWindow);
		}
	} else {
		windowState.setCurrentNamespace(triggerContext.window);
		mergingWindows = null;
	}

	TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

	if (triggerResult.isFire()) {
		ACC contents = windowState.get();
		if (contents != null) {
			emitWindowContents(triggerContext.window, contents);
		}
	}

	if (triggerResult.isPurge()) {
		windowState.clear();
	}

	if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
		clearAllState(triggerContext.window, windowState, mergingWindows);
	}

	if (mergingWindows != null) {
		// need to make sure to update the merging state in state
		mergingWindows.persist();
	}
}
```

## 窗口的回收器

在设置了回收器```Evictor```后，窗口的逻辑实现由```WindowOperator```变成```EvictingWindowOperator```，其主要变化包括两个部分：
1. 使用```evictingWindowState```代替了原先```windowState```来管理窗口状态
2. 在```EvictingWindowOperator#emitWindowContent```方法的实现部分增加了回收器的处理流程

### 回收器的实现

回收器包括两个部分：具体逻辑实现（实现Evictor接口）和回收器上下文（实现Evictor.EvictorContext接口）

```java
public interface Evictor<T, W extends Window> extends Serializable {

	void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

	void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

	interface EvictorContext {

		long getCurrentProcessingTime();

		MetricGroup getMetricGroup();

		long getCurrentWatermark();
	}
}
```

```Evictor#evictBefore```方法对应在窗口计算前执行窗口元素的回收，```Evictor#evictAfter```方法对应在窗口计算后执行元素的回收。

Flink提供了```TimeEvictor```（回收超过最大keepTime的元素）、```CountEvictor```（回收超过maxCount的元素）和```DeltaEvictor```（回收超过差值阈值的元素）的实现。

### 回收器的触发

回收器的触发在触发器返回```FIRE```（或```FIRE_AND_PURGE```）后，即```EvictingWindowOperator#emitWindowContents```方法的执行逻辑内部：

```java
// EvictingWindowOperator.class第336行
private void emitWindowContents(W window, Iterable<StreamRecord<IN>> contents, ListState<StreamRecord<IN>> windowState) throws Exception {
	timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

	// Work around type system restrictions...
	FluentIterable<TimestampedValue<IN>> recordsWithTimestamp = FluentIterable
		.from(contents)
		.transform(new Function<StreamRecord<IN>, TimestampedValue<IN>>() {
			@Override
			public TimestampedValue<IN> apply(StreamRecord<IN> input) {
				return TimestampedValue.from(input);
			}
		});
	evictorContext.evictBefore(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

	FluentIterable<IN> projectedContents = recordsWithTimestamp
		.transform(new Function<TimestampedValue<IN>, IN>() {
			@Override
			public IN apply(TimestampedValue<IN> input) {
				return input.getValue();
			}
		});

	processContext.window = triggerContext.window;
	userFunction.process(triggerContext.key, triggerContext.window, processContext, projectedContents, timestampedCollector);
	evictorContext.evictAfter(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

	//work around to fix FLINK-4369, remove the evicted elements from the windowState.
	//this is inefficient, but there is no other way to remove elements from ListState, which is an AppendingState.
	windowState.clear();
	for (TimestampedValue<IN> record : recordsWithTimestamp) {
		windowState.add(record.getStreamRecord());
	}
}
```

回收器不直接改变窗口的状态，仅对窗口状态导出的窗口元素值迭代器进行修改，再由窗口算子回填更新到窗口状态中。

## 参考文献

1. [Windows](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/windows.html#windowfunction-legacy)
2. [流式计算系统系列（3）：窗口](https://zhuanlan.zhihu.com/p/103890281)