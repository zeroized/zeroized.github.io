# Window(3): 窗口的状态
2020/10/16

在前两篇[Window(1): 窗口的分配](/engineering/flink/window1.md)和[Window(2): 触发器与回收器](/engineering/flink/window2.md)中，我们分析了窗口分配器、触发器和回收器，这三者都不拥有窗口状态，但都会导致窗口状态的更新。本篇将解析窗口状态的实现以及窗口算子维护状态的过程。

## 无回收器的窗口状态

窗口的状态由窗口算子维护，因此在[Window(1): 窗口的分配](/engineering/flink/window1.md)中提到了“窗口并不真正拥有数据”这一概念（实际从代码实现看，只是窗口类的实例没有拥有数据，在窗口状态中维护了窗口的```namespace```$\to$属于该窗口元素的值列表的映射表，实际上是“窗口并不真正拥有数据本身，只拥有数据的内容”）。

```java
// WindowOperator.class第150行
private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;
```

```windowState```维护了每个窗口中存储的数据元素的**值**，```windowMergingState```维护了合并窗口的合并状态，```mergingSetsState```维护了合并窗口的元数据，后两者仅在需要合并窗口的场景下会实际使用（```windowAssigner instanceof MergingWindowAssigner```）。

### 窗口状态的实现

窗口状态的初始化位于```StreamOperator#open```方法中：

```java
// WindowOperator.class第239行
// open()方法
if (windowStateDescriptor != null) {
	windowState = (InternalAppendingState<K, W, IN, ACC, ACC>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
}
```

```windowState```既是一个键值对状态也是一个可追加状态（添加新的元素会追加到一个类似列表的缓存，或是被合并到一个累加器中），其工作方式为先设置状态的```namespace```（即指定Key），然后调用```AppendingState#add```方法更新状态时会向设置的```namespace```中追加一个元素。

### 处理数据元素的状态变化

在处理数据元素时，将新数据元素的值添加到其所在的窗口中；在触发器计算结果后，如果是触发则取出该窗口的所有值执行窗口计算方法，如果是清除则清除该窗口对应状态```namespace```中所有元素。

```java
// WindowOperator.class第385行
// processElement(StreamRecord<IN>) throws Exception方法，非合并窗口分支
for (W window: elementWindows) {

	// drop if the window is already late
	isSkippedElement = false;

	windowState.setCurrentNamespace(window);
	windowState.add(element.getValue());

	triggerContext.key = key;
	triggerContext.window = window;

	TriggerResult triggerResult = triggerContext.onElement(element);

	if (triggerResult.isFire()) {
		ACC contents = windowState.get();
		if (contents == null) {
			continue;
		}
		emitWindowContents(window, contents);
	}

	if (triggerResult.isPurge()) {
		windowState.clear();
	}
	registerCleanupTimer(window);
}
```

### 时间触发的状态变化

由时间触发（Processing Time定时任务触发```WindowOperator#onProcessingTime```/Watermark触发```WindowOperator#onEventTime```，见[Time & Watermark(2): Watermark的传播与处理](/engineering/flink/time2.md)）的状态变化与前面```WindowOperator#processElement```中触发器计算后的部分基本一致，根据触发器计算的结果决定取出状态进行计算还是清除状态。在此之后，算子将计算触发计算的窗口是否已经过期（Processing Time为窗口最大时间戳，Event Time为窗口最大时间戳+允许晚到的最大间隔），然后清除过期窗口的所有状态。

```java
// WindowOperator.class
// onEventTime(InternalTimer<K, W>)
else {
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

// 与上一个分支功能相同的onProcessingTime方法版本
if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
    clearAllState(triggerContext.window, windowState, mergingWindows);
}
```

## Reduce, Fold, Aggregate和Process Function的性能差异

官方文档中在ProcessWindowFunction的介绍末尾，添加了如下一段话：

> Note that using ProcessWindowFunction for simple aggregates such as count is quite inefficient. The next section shows how a ReduceFunction or AggregateFunction can be combined with a ProcessWindowFunction to get both incremental aggregation and the added information of a ProcessWindowFunction.

这个性能的差别是由```WindowOperator```实现```windowState```使用了不同的```InternalAppendingState```实现导致的（注意，当使用回收器时，所有的4种Function均使用了```ListState```，使用```ProcessWindowFunction```进行reduce和aggregate计算理论上不会损失性能）。Reduce和Aggregate Function使用了ReduceState，而Fold和Process Function使用了ListState。

## 合并窗口状态

```java
// WindowOperator.class第
private transient InternalMergingState<K, W, IN, ACC, ACC> windowMergingState;

private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;
```

## 带有回收器的窗口状态

带有回收器的窗口算子```EvictingWindowOperator```使用```evictingWindowState```在逻辑上替代了```windowState```，并使用null初始化```windowState```。

```java
private transient InternalListState<K, W, StreamRecord<IN>> evictingWindowState;
```
