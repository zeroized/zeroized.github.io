# Window(3): 窗口的状态
2020/10/16

在前两篇[Window(1): 窗口的分配](/engineering/flink/window1.md)和[Window(2): 触发器与回收器](/engineering/flink/window2.md)中，我们分析了窗口分配器、触发器和回收器，这三者都不拥有窗口状态，但都会导致窗口状态的更新。本篇将解析窗口状态的实现以及窗口算子维护状态的过程。

## 无回收器的窗口状态

窗口的状态由窗口算子维护，因此在[Window(1): 窗口的分配](/engineering/flink/window1.md)中提到了“窗口并不真正拥有数据”这一概念（实际从代码实现看，只是窗口类的实例没有拥有数据，在窗口状态中维护了窗口的```namespace```$\to$属于该窗口元素的值列表的映射表，是“窗口并不真正拥有数据本身，只拥有数据的内容”）。

```java
// WindowOperator.class第150行
private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;
```

```windowState```维护了每个窗口中存储的数据元素的**值**，```windowMergingState```维护了合并窗口的合并状态，```mergingSetsState```维护了合并窗口的元数据，后两者仅在需要合并窗口的场景下会实际使用（```windowAssigner instanceof MergingWindowAssigner```）。

### 窗口状态的实现

窗口状态的初始化位于```StreamOperator#open```方法中：

```java
// WindowOperator.class第239行
// open() throws Exception方法
if (windowStateDescriptor != null) {
	windowState = (InternalAppendingState<K, W, IN, ACC, ACC>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
}
```

```windowState```既是一个键值对状态也是一个可追加状态（添加新的元素会追加到一个类似列表的缓存，或是被合并到一个累加器中），其工作方式为先设置状态的```namespace```（即指定Key），然后调用```AppendingState#add```方法更新状态时会向设置的```namespace```中追加一个元素。

具体的状态实现类选择的调用栈如下：
- ```WindowOperator#open```
- ```AbstractStreamOperator#getOrCreateKeyedState```
- ```StreamOperatorStateHandler#getOrCreateKeyedState```
- ```AbstractKeyedStateBackend#getOrCreateKeyedState```
- ```TtlStateFactory#createStateAndWrapWithTtlIfEnabled```

然后根据系统是否设置了启用ttl和状态描述器的类型，会生成```TtlState```（HeapState的一层封装）或```HeapState```：
```java
// TtlStateFactory.class第59行
public static <K, N, SV, TTLSV, S extends State, IS extends S> IS createStateAndWrapWithTtlIfEnabled(
	TypeSerializer<N> namespaceSerializer,
	StateDescriptor<S, SV> stateDesc,
	KeyedStateBackend<K> stateBackend,
	TtlTimeProvider timeProvider) throws Exception {
	Preconditions.checkNotNull(namespaceSerializer);
	Preconditions.checkNotNull(stateDesc);
	Preconditions.checkNotNull(stateBackend);
	Preconditions.checkNotNull(timeProvider);
	return  stateDesc.getTtlConfig().isEnabled() ?
		new TtlStateFactory<K, N, SV, TTLSV, S, IS>(
			namespaceSerializer, stateDesc, stateBackend, timeProvider)
			.createState() :
		stateBackend.createInternalState(namespaceSerializer, stateDesc);
}
```

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

这个性能的差别是由```WindowOperator```实现```windowState```使用了不同的```InternalAppendingState```实现导致的（注意，当使用回收器时（```evictor!=null```），所有的4种Function均使用了```ListState```，使用```ProcessWindowFunction```进行reduce和aggregate计算理论上不会损失性能）。Reduce、Aggregate、Fold和Process Function分别使用了```HeapReducingState```、```HeapAggregatingState```、```HeapFoldingState```和```HeapListState```作为实际的State实现（HeapFoldingState已被标记为```@Deprecated```并将用```HeapAggregatingState```代替）。

### ReducingState

我们来看下```HeapReducingState```在```windowState.add()```时内部发生了什么：

```java
// HeapReducingState.class第93行
@Override
public void add(V value) throws IOException {

	if (value == null) {
		clear();
		return;
	}

	try {
		stateTable.transform(currentNamespace, value, reduceTransformation);
	} catch (Exception e) {
		throw new IOException("Exception while applying ReduceFunction in reducing state", e);
	}
}
```

后续的调用栈为：
- ```StableTable#transform```
- ```StateMap#transform```
- ```StateTransformationFunction#apply```
- ```ReduceFunction#reduce```

其中```ReduceFunction```的描述是：

> @return The combined value of both input values.

可以看到，当执行```windowState.addWindow(T)```时，状态的更新已经完成了```ReduceFunction```的计算。在获取整个窗口的状态时，得到的是```reducing```后的结果：
```java
// HeapReducingState.class第88行
@Override
public V get() {
	return getInternal();
}
```

### AggregatingState

AggregatingState的工作流程与ReducingState基本一致。唯一的不同点是，由于Aggregating操作的计算结果类型可能与数据元素类型一致，在最后需要对累加器的结果进行一次转换：

```java
// HeapAggregatingState.class第90行	
@Override
public OUT get() {
	ACC accumulator = getInternal();
	return accumulator != null ? aggregateTransformation.aggFunction.getResult(accumulator) : null;
}
```

### ListState

而ListState无论是过程还是类的构造都简单得多，其中```AppendingState#add```的实现仅仅是向目标```namespace```的列表中添加一个新的元素（不存在则创建）；在获取整个窗口状态时，得到的也是整个列表：

```java
// HeapListState.class第89行
@Override
public void add(V value) {
	Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

	final N namespace = currentNamespace;

	final StateTable<K, N, List<V>> map = stateTable;
	List<V> list = map.get(namespace);

	if (list == null) {
		list = new ArrayList<>();
		map.put(namespace, list);
	}
	list.add(value);
}

// HeapListState.class第84行
@Override
public Iterable<V> get() {
	return getInternal();
}
```

这就意味着，使用```ProcessWindowFunction```进行窗口处理，在元素进入窗口时仅会被追加到窗口状态的末尾；而不会像R```educingFunction```和```AggregatingFunction```一样对每个进入窗口的元素立即进行聚合。

## 合并窗口状态

在合并窗口场景下，窗口算子会拥有两个额外的状态：

```java
// WindowOperator.class第156行
private transient InternalMergingState<K, W, IN, ACC, ACC> windowMergingState;

private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

// WindowOperator.class第244行
// open() throws Exception方法
if (windowAssigner instanceof MergingWindowAssigner) {

	// store a typed reference for the state of merging windows - sanity check
	if (windowState instanceof InternalMergingState) {
		windowMergingState = (InternalMergingState<K, W, IN, ACC, ACC>) windowState;
	}

	else if (windowState != null) {
		throw new IllegalStateException(
				"The window uses a merging assigner, but the window state is not mergeable.");
	}

	@SuppressWarnings("unchecked")
	final Class<Tuple2<W, W>> typedTuple = (Class<Tuple2<W, W>>) (Class<?>) Tuple2.class;

	final TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>(
			typedTuple,
			new TypeSerializer[] {windowSerializer, windowSerializer});

	final ListStateDescriptor<Tuple2<W, W>> mergingSetsStateDescriptor =
			new ListStateDescriptor<>("merging-window-set", tupleSerializer);

	// get the state that stores the merging sets
	mergingSetsState = (InternalListState<K, VoidNamespace, Tuple2<W, W>>)
			getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mergingSetsStateDescriptor);
	mergingSetsState.setCurrentNamespace(VoidNamespace.INSTANCE);
}
```

其中，```windowMergingState```用于记录合并窗口的状态，是```windowState```的一个用于合并的引用，在触发窗口合并时，会执行窗口状态的合并方法```InternalMergingState#mergeNamespaces```，执行ReducingState或AggregatingState的```ReducingFunction```/```AggregatingFunction```，或是执行ListState的```List#addAll```。

```mergingSetsState```则是记录了窗口的元数据。```mergingSetsState```的变更位于```MergeWindowSet#persist```方法处（```MergeWindowSet```的```state```变量即```mergingSetsState```的引用），在合并过程中的状态变化见[Window(1): 窗口的分配](/engineering/flink/window1.md)中对窗口合并的相关描述。

```java
// MergingWindowSet.class第102行
public void persist() throws Exception {
	if (!mapping.equals(initialMapping)) {
		state.clear();
		for (Map.Entry<W, W> window : mapping.entrySet()) {
			state.add(new Tuple2<>(window.getKey(), window.getValue()));
		}
	}
}
```

## 带有回收器的窗口状态

带有回收器的窗口算子```EvictingWindowOperator```使用```evictingWindowState```在逻辑上替代了```windowState```，并使用null初始化```windowState```（同时也导致```windowMergingState```也不再使用，直接用```evictingWindowState```而不是另一个引用记录合并窗口状态）。使用回收器时，状态一定为ListState。

```java
// EvictingWindowOperator.class第79行
private transient InternalListState<K, W, StreamRecord<IN>> evictingWindowState;

// EvictingWindowOperator.class第83行
public EvictingWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> windowStateDescriptor,
		InternalWindowFunction<Iterable<IN>, OUT, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger,
		Evictor<? super IN, ? super W> evictor,
		long allowedLateness,
		OutputTag<IN> lateDataOutputTag) {

	super(windowAssigner, windowSerializer, keySelector,
		keySerializer, null, windowFunction, trigger, allowedLateness, lateDataOutputTag);

	this.evictor = checkNotNull(evictor);
	this.evictingWindowStateDescriptor = checkNotNull(windowStateDescriptor);
}

// EvictingWindowOperator.class第429行
@Override
public void open() throws Exception {
	super.open();

	evictorContext = new EvictorContext(null, null);
	evictingWindowState = (InternalListState<K, W, StreamRecord<IN>>)
			getOrCreateKeyedState(windowSerializer, evictingWindowStateDescriptor);
}
```

带有回收器的窗口算子的合并窗口流程与无回收器算子一致，均为```MergeWindowSet#persist```方法。

由于回收器在执行窗口计算方法前后会回收窗口中的元素，因此在触发器返回```FIRE```后，即```EvictingWindowOperator#emitWindowContents```处需要对窗口的状态进行更新，见[Window(2): 触发器与回收器](/engineering/flink/window2.md)中对回收器的触发的相关描述。