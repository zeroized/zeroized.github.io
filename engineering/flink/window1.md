# Window(1): 窗口的分配、触发与回收
2020/10/15

在[Time & Watermark(1): Flink中Watermark的生成](/engineering/flink/time1.md)和[Time & Watermark(2): Watermark的传播与处理](/engineering/flink/time2.md)中介绍了Flink是如何处理Event Time和Watermark的。作为流式计算的核心概念之一，同时也是Time最重要的应用场景，本篇将主要分析窗口的运行方式。

注：源代码为Flink1.11.0版本

## Flink中的窗口

在Flink中，根据数据流是否是Keyed，又分为keyed window（通过```KeyedStream#window```）和non-keyed window（通过```DataStream#windowAll```），这两者区别在于，在keyed window中，不同key对应的窗口之间是完全独立的（概念上），因此每个窗口在相同的配置下可能具有不同的触发时间。 这两种window分别对应```WindowedStream```和```AllWindowedStream```，虽然在实现上有所不同，但其构成是一样的，包括4个部分：窗口分配器```WindowAssigner```、触发器```Trigger```、回收器```Evictor```（非必须）和窗口计算方法```ProcessWindowFunction```，视```Evictor```是否存在由```WindowOperator```和```EvictingWindowOperator```这两个算子具体实现。

```java
// WindowedStream.class & AllWindowedStream.class, reduce/aggregate/fold/apply
if (evictor != null) {
	// do something according to function

	operator = new EvictingWindowOperator<>(...);

} else {
    // do something according to function

	operator = new WindowOperator<>(...);
}
```

### 窗口的生命周期

当窗口分配器将属于窗口的第一个数据元素分配给窗口时，一个新的窗口被创建。当时间（Processing Time或Watermark）超过窗口的结束时间戳（或结束时间戳+最大允许晚到时间），窗口将会被完全移除。窗口的触发器能够决定一个进入窗口的元素是否触发窗口计算或是清除窗口中的数据元素元素（仅清除数据元素而不是窗口本身，后续的数据元素依然可以进入窗口），而回收器可以在触发器触发后、窗口计算运行前/后执行[1]。

### 窗口的实现

在Flink中，窗口并不真正拥有数据——窗口不暂存数据，而是由数据自身决定属于哪个或哪些窗口[2]：

```java
// Window.class
public abstract class Window {

	public abstract long maxTimestamp();
}
```

根据窗口自身是否具有时间信息，分为```TimeWindow```和```GlobalWindow```（GlobalWindow的最大时间戳为```Long.MAX_VALUE```，是Watermark的结束标识符）

```java
public class TimeWindow extends Window {

	private final long start;
	private final long end;

	public TimeWindow(long start, long end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public long maxTimestamp() {
		return end - 1;
	}
    
    // getters, equals, hashCode, toString, Serializer, SimpleTypeSerializerSnapshot, intersects(), cover(), mergeWindows()
}

public class GlobalWindow extends Window {

	private static final GlobalWindow INSTANCE = new GlobalWindow();

	private GlobalWindow() { }

	public static GlobalWindow get() {
		return INSTANCE;
	}

	@Override
	public long maxTimestamp() {
		return Long.MAX_VALUE;
	}

    // equals, hashCode, toString, Serializer, SimpleTypeSerializerSnapshot
}
```

## 窗口的分配

当一个数据元素进入算子后，首先由一个窗口分配器```WindowAssigner```为其分配所属的窗口：

```java
// WindowAssigner.class
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
	private static final long serialVersionUID = 1L;

	public abstract Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context);

	public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);

	public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);

	public abstract boolean isEventTime();

	public abstract static class WindowAssignerContext {

		public abstract long getCurrentProcessingTime();

	}
}
```

每个```WindowAssigner```都有默认的触发器，因此在创建窗口处理过程时不是必须要调用```[All]WindowedStream#trigger```。根据Flink官方文档和源代码中的实现类，内置的窗口分配方法可以分为：Tumbling Window滚动窗口、Sliding Window滑动窗口、Session Window会话窗口、动态会话窗口和Global Window全局窗口，又根据默认的时间触发器类型有Processing Time还是Event Time，总共有9种内置分配器（全局窗口不区分时间类型）。

全局窗口只有一个窗口：

<details>
<summary>全局窗口</summary>

```java
// GlobalWindows.class第47行
@Override
public Collection<GlobalWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
	return Collections.singletonList(GlobalWindow.get());
}
```
</details>

滚动窗口和滑动窗口在配置完成后就已经固定了，每个窗口的起始时间会被固定为距离```timestamp-offset```最近的```windowSize```整数倍时间戳：

<details>
<summary>滚动窗口</summary>

```java
// TumblingEventTimeWindows.class
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
	if (timestamp > Long.MIN_VALUE) {
		// Long.MIN_VALUE is currently assigned when no timestamp is present
		long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
		return Collections.singletonList(new TimeWindow(start, start + size));
	} else {
		throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
				"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
				"'DataStream.assignTimestampsAndWatermarks(...)'?");
	}
}

// TumblingProcessingTimeWindows.class
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
	final long now = context.getCurrentProcessingTime();
	long start = TimeWindow.getWindowStartWithOffset(now, offset, size);
	return Collections.singletonList(new TimeWindow(start, start + size));
}
```
</details>

<details>
<summary>滑动窗口</summary>

```java
// SlidingEventTimeWindows.class
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
	if (timestamp > Long.MIN_VALUE) {
		List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
		long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
		for (long start = lastStart;
			start > timestamp - size;
			start -= slide) {
			windows.add(new TimeWindow(start, start + size));
		}
		return windows;
	} else {
		throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
				"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
				"'DataStream.assignTimestampsAndWatermarks(...)'?");
	}
}

// SlidingProcessingTimeWindows.class
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
	timestamp = context.getCurrentProcessingTime();
	List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
	long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
	for (long start = lastStart;
		start > timestamp - size;
		start -= slide) {
		windows.add(new TimeWindow(start, start + size));
	}
	return windows;
}
```
</details>

会话窗口和动态会话窗口在创建时，每次总是创建一个新的以事件的时间戳为起点、会话过期时间（动态会话的过期时间由数据元素的时间戳/Processing Time通过```SessionWindowTimeGapExtractor#extract```方法得到）为终点的窗口，然后检查窗口之间是否有重叠并合并重叠窗口。

<details>
<summary>会话窗口</summary>

```java
// EventTimeSessionWindows.class第59行
@Override
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
	return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
}

// ProcessingTimeSessionWindows.class第59行
@Override
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
	long currentProcessingTime = context.getCurrentProcessingTime();
	return Collections.singletonList(new TimeWindow(currentProcessingTime, currentProcessingTime + sessionTimeout));
}
```
</details>

<details>
<summary>动态会话窗口</summary>

```java
// DynamicEventTimeSessionWindows.class第57行
@Override
public Collection<TimeWindow> assignWindows(T element, long timestamp, WindowAssignerContext context) {
	long sessionTimeout = sessionWindowTimeGapExtractor.extract(element);
	if (sessionTimeout <= 0) {
		throw new IllegalArgumentException("Dynamic session time gap must satisfy 0 < gap");
	}
	return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
}

// DynamicProcessingTimeSessionWindows.class第57行
@Override
public Collection<TimeWindow> assignWindows(T element, long timestamp, WindowAssignerContext context) {
	long currentProcessingTime = context.getCurrentProcessingTime();
	long sessionTimeout = sessionWindowTimeGapExtractor.extract(element);
	if (sessionTimeout <= 0) {
		throw new IllegalArgumentException("Dynamic session time gap must satisfy 0 < gap");
	}
	return Collections.singletonList(new TimeWindow(currentProcessingTime, currentProcessingTime + sessionTimeout));
}
```
</details>

### 窗口的合并

在（动态）会话窗口场景下，一个新数据元素的到来可能会触发重叠窗口的合并。合并状态保存于```WindowOperator```的```mergingSetsState```变量中，是一个```ListState```状态（注意，不是```windowMergingState```，```windowMergingState```是用于保存窗口状态的State变量）。窗口合并的过程使用```MergingWindowSet```工具类记录合并前和合并后的窗口状态，并以```WindowOperator#processElement```$\to$```MergingWindowSet#addWindow```$\to$```MergingWindowAssigner#mergeWindows```$\to$```TimeWindow#mergeWindows```的顺序执行了合并过程，其中```MergingWindowAssigner#mergeWindows```是```MergingWindowAssigner```对```TimeWindow#mergeWindows```的一层封装。我们按这个顺序反过来看合并窗口的过程：

```java
// TimeWindow.class第120行
public boolean intersects(TimeWindow other) {
	return this.start <= other.end && this.end >= other.start;
}

public TimeWindow cover(TimeWindow other) {
	return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
}

// TimeWindow.class第217行
public static void mergeWindows(Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {

	// sort the windows by the start time and then merge overlapping windows

	List<TimeWindow> sortedWindows = new ArrayList<>(windows);

	Collections.sort(sortedWindows, new Comparator<TimeWindow>() {
		@Override
		public int compare(TimeWindow o1, TimeWindow o2) {
			return Long.compare(o1.getStart(), o2.getStart());
		}
	});

	List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
	Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

	for (TimeWindow candidate: sortedWindows) {
		if (currentMerge == null) {
			currentMerge = new Tuple2<>();
			currentMerge.f0 = candidate;
			currentMerge.f1 = new HashSet<>();
			currentMerge.f1.add(candidate);
		} else if (currentMerge.f0.intersects(candidate)) {
			currentMerge.f0 = currentMerge.f0.cover(candidate);
			currentMerge.f1.add(candidate);
		} else {
			merged.add(currentMerge);
			currentMerge = new Tuple2<>();
			currentMerge.f0 = candidate;
			currentMerge.f1 = new HashSet<>();
			currentMerge.f1.add(candidate);
		}
	}

	if (currentMerge != null) {
		merged.add(currentMerge);
	}

	for (Tuple2<TimeWindow, Set<TimeWindow>> m: merged) {
		if (m.f1.size() > 1) {
			c.merge(m.f1, m.f0);
		}
	}
}
```

窗口合并的过程非常简单，首先将窗口按照其起始时间升序排序，然后从第一个窗口开始逐步“吸收”后一个窗口：判断前后两个窗口是否相交（```intersects(TimeWindow)```），如果相交则覆盖后一个窗口（```cover(TimeWindow)```）。合并完成后得到一个二元组列表，元素1对应合并后的窗口，元素2对应哪些窗口和并得到了这个新窗口。最后对所有合并得到的新窗口（元素2的集合元素个数大于1）执行合并回调逻辑。然后，我们来看执行窗口合并的上下文和回调逻辑：

```java
// MergingWindowSet.class第156行
public W addWindow(W newWindow, MergeFunction<W> mergeFunction) throws Exception {

	List<W> windows = new ArrayList<>();

	windows.addAll(this.mapping.keySet());
	windows.add(newWindow);

	final Map<W, Collection<W>> mergeResults = new HashMap<>();
	windowAssigner.mergeWindows(windows,
			new MergingWindowAssigner.MergeCallback<W>() {
				@Override
				public void merge(Collection<W> toBeMerged, W mergeResult) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Merging {} into {}", toBeMerged, mergeResult);
					}
					mergeResults.put(mergeResult, toBeMerged);
				}
			});

	W resultWindow = newWindow;
	boolean mergedNewWindow = false;

	// perform the merge
	for (Map.Entry<W, Collection<W>> c: mergeResults.entrySet()) {
		W mergeResult = c.getKey();
		Collection<W> mergedWindows = c.getValue();

		// if our new window is in the merged windows make the merge result the
		// result window
		if (mergedWindows.remove(newWindow)) {
			mergedNewWindow = true;
			resultWindow = mergeResult;
		}

		// pick any of the merged windows and choose that window's state window
		// as the state window for the merge result
		W mergedStateWindow = this.mapping.get(mergedWindows.iterator().next());

		// figure out the state windows that we are merging
		List<W> mergedStateWindows = new ArrayList<>();
		for (W mergedWindow: mergedWindows) {
			W res = this.mapping.remove(mergedWindow);
			if (res != null) {
				mergedStateWindows.add(res);
			}
		}

		this.mapping.put(mergeResult, mergedStateWindow);

		// don't put the target state window into the merged windows
		mergedStateWindows.remove(mergedStateWindow);

		// don't merge the new window itself, it never had any state associated with it
		// i.e. if we are only merging one pre-existing window into itself
		// without extending the pre-existing window
		if (!(mergedWindows.contains(mergeResult) && mergedWindows.size() == 1)) {
			mergeFunction.merge(mergeResult,
					mergedWindows,
					this.mapping.get(mergeResult),
					mergedStateWindows);
		}
	}

	// the new window created a new, self-contained window without merging
	if (mergeResults.isEmpty() || (resultWindow.equals(newWindow) && !mergedNewWindow)) {
		this.mapping.put(resultWindow, resultWindow);
	}

	return resultWindow;
}
```

```MergingWindowSet```在回调方法```MergingWindowAssigner.MergeCallback```中记录了进行合并的窗口并输出日志。在执行实际的合并后，```MergingWindowSet```遍历合并结果列表并依次执行以下过程以更新状态（用名为```mapping```的```HashMap```实例变量保存）：
1. 判断新加入的窗口是否被合并，并修改返回结果
2. 在```mapping```中移除所有被合并的窗口
3. 在```mapping```中加入合并后的窗口，并将被合并的窗口中任意一个的状态作为其状态窗口
4. 执行MergeFunction

如果新加入的窗口没有被合并，那么在状态中保存该窗口。从返回结果上看，``addWindow(W, MergeFunction<W>)``方法返回新窗口经过合并后得到的窗口，如果没有进行合并则返回新窗口自身。

```java
// WindowOperator.class#processElement第295行
final Collection<W> elementWindows = windowAssigner.assignWindows(
	element.getValue(), element.getTimestamp(), windowAssignerContext);

//if element is handled by none of assigned elementWindows
boolean isSkippedElement = true;

final K key = this.<K>getKeyedStateBackend().getCurrentKey();

if (windowAssigner instanceof MergingWindowAssigner) {
	MergingWindowSet<W> mergingWindows = getMergingWindowSet();

	for (W window: elementWindows) {

		// adding the new window might result in a merge, in that case the actualWindow
		// is the merged window and we work with that. If we don't merge then
		// actualWindow == window
		W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
			@Override
			public void merge(W mergeResult,
					Collection<W> mergedWindows, W stateWindowResult,
					Collection<W> mergedStateWindows) throws Exception {

				if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
					throw new UnsupportedOperationException("The end timestamp of an " +
							"event-time window cannot become earlier than the current watermark " +
							"by merging. Current watermark: " + internalTimerService.currentWatermark() +
							" window: " + mergeResult);
				} else if (!windowAssigner.isEventTime()) {
					long currentProcessingTime = internalTimerService.currentProcessingTime();
					if (mergeResult.maxTimestamp() <= currentProcessingTime) {
						throw new UnsupportedOperationException("The end timestamp of a " +
							"processing-time window cannot become earlier than the current processing time " +
							"by merging. Current processing time: " + currentProcessingTime +
							" window: " + mergeResult);
					}
				}

				triggerContext.key = key;
				triggerContext.window = mergeResult;

				triggerContext.onMerge(mergedWindows);

				for (W m: mergedWindows) {
					triggerContext.window = m;
					triggerContext.clear();
					deleteCleanupTimer(m);
				}

				// merge the merged state windows into the newly resulting state window
				windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
			}
		});

		// drop if the window is already late
		if (isWindowLate(actualWindow)) {
			mergingWindows.retireWindow(actualWindow);
			continue;
		}
        // do something with trigger and evictor
	}

	// need to make sure to update the merging state in state
	mergingWindows.persist();
} else {
    // do something with other kinds of window assigner
}
```


最后，在WindowOperator层面，合并窗口将更新并清除被合并窗口的触发器上下文，并将这些窗口的状态合并并更新到算子状态```windowMergingState```中

## 参考文献

1. [Windows](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/windows.html#windowfunction-legacy)
2. [流式计算系统系列（3）：窗口](https://zhuanlan.zhihu.com/p/103890281)