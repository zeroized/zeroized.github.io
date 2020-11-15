# State(2): 状态的实现(下)
2020/11/07

在[State(1): 状态的实现（上）](/engineering/flink/state1.md)中介绍了keyed状态初始化、创建和获取的实现、以及Time-To-Live机制的工作方式。本篇将研究无论是是keyed数据流还是non-keyed数据流都能使用的算子状态的原理。

注：源代码为Flink1.11.0版本

## Operator State

相比keyed状态，算子状态更重要的作用是容错和重规划，算子状态的模式决定了算子在容错恢复和重规划时的状态分配，共有even-split、union和broadcast三种模式。even-split和union分发模式使用List State实现，broadcast模式在BroadcastConnectedStream（一般数据流与广播数据流连接）中使用Broadcast State实现，在一般non-keyed数据流中直接将元素发送到下游所有partition中。

### 状态的初始化

算子状态的初始化和keyed状态基本一致，是在```AbstractStreamOperator#initializeState```(```StreamOperator```接口)方法中：

```java
// AbstractStreamOperator.class第237行
public final void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {

	final TypeSerializer<?> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());

	final StreamTask<?, ?> containingTask =
		Preconditions.checkNotNull(getContainingTask());
	final CloseableRegistry streamTaskCloseableRegistry =
		Preconditions.checkNotNull(containingTask.getCancelables());

	final StreamOperatorStateContext context =
		streamTaskStateManager.streamOperatorStateContext(
			getOperatorID(),
			getClass().getSimpleName(),
			getProcessingTimeService(),
			this,
			keySerializer,
			streamTaskCloseableRegistry,
			metrics);

	stateHandler = new StreamOperatorStateHandler(context, getExecutionConfig(), streamTaskCloseableRegistry);
	timeServiceManager = context.internalTimerServiceManager();
	stateHandler.initializeOperatorState(this);
	runtimeContext.setKeyedStateStore(stateHandler.getKeyedStateStore().orElse(null));
}
```

与```keyedStateStore```和```keyedStateBackend```略有不同的是，```OperatorStateBackend```是```OperatorStateStore```的加强版本（增加了SnapshotStrategy），两者是同一个东西。```OperatorStateBackend```的初始化主要在```stateHandler.initializeOperatorState(this)```这一行，其中调用了```AbstractStreamOperator#initializeState```(```CheckpointedStreamOperator```接口)方法，该方法在```AbstractStreamOperator```默认不进行任何操作。由于算子状态直接影响到checkpoint和rebalance、rescale的重划分，不同算子对算子状态的要求也不尽相同，对算子状态使用有需求的功能算子自行实现该方法。

### 算子状态的获取

使用算子状态需要实现````CheckpointedFunction````接口，该接口提供了初始化状态和快照状态两个方法：

```java
// CheckpointedFunction.class
public interface CheckpointedFunction {

	void snapshotState(FunctionSnapshotContext context) throws Exception;

	void initializeState(FunctionInitializationContext context) throws Exception;
}
```

其中，```snapshotState```方法在每次进行checkpoint时会执行；而```initializeState```方法在每次初始化UDF时会执行，包括算子初始化时以及从故障中恢复时。可以通过初始化状态方法提供的参数```FunctionInitializationContext#getOperatorStateStore```获取到算子状态，然后通过```DefaultOperatorStateBackend#getXXXState```（```DefaultOperatorStateBackend```是目前```OperatorStateStore```接口的唯一实现）可以获取到三种模式对应的List（即even-split模式）、Union和Broadcast状态（注意，Broadcast状态不完全对应broadcast模式，non-keyed数据流可以通过```DataStream.broadcast()```方法变为broadcast模式）。这三种状态与keyed State无论是结构还是用途都完全不同。

## Broadcast状态

首先我们来看一下```OperatorStateStore```接口中```getBroadcastState()```方法的相关描述：

> Creates (or restores) a {@link BroadcastState broadcast state}. This type of state can only be created to store the state of a {@code BroadcastStream}. Each state is registered under a unique name. The provided serializer is used to de/serialize the state in case of checkpointing (snapshot/restore). The returned broadcast state has {@code key-value} format.
> 
> **CAUTION: the user has to guarantee that all task instances store the same elements in this type of state.**
>
> Each operator instance individually maintains and stores elements in the broadcast state. The fact that the incoming stream is a broadcast one guarantees that all instances see all the elements. Upon recovery or re-scaling, the same state is given to each of the instances. To avoid hotspots, each task reads its previous partition, and if there are more tasks (scale up), then the new instances read from the old instances in a round robin fashion. This is why each instance has to guarantee that it stores the same elements as the rest. If not, upon recovery or rescaling you may have unpredictable redistribution of the partitions, thus unpredictable results.

实际上broadcast是在广播流上游算子向下游算子的partition发送数据时进行的广播，Broadcast状态只能保证所有的partition能够看到广播流中所有的数据，**不能保证每个算子中的Broadcast状态是一定一致的**。但如果我们先验地**假设**算子的所有partition在看到广播流中的元素能够做出相同的状态更新，由于Flink底层使用TCP保证了数据顺序不会发生变化，就可以认为每个算子中的Broadcast状态是完全一致的，因此在源码中警告用户需要保证对广播状态的更新在每个task实例中是完全一致的。

Broadcast模式的重规划方法是```RoundRobinOperatorStateRepartitioner#repartitionBroadcastState```，将在状态篇后续的文章中介绍。

### Broadcast状态的实现

Broadcast状态对应的实现为```HeapBroadcastState```，是```BroadcastState```接口的唯一实现，并增加了将状态写入后端的相关实现（```BackendWritableBroadcastState```接口）。实际上BroadcastState比InternalState的实现要更简单，基本上就是对内部存储的```backingMap```的封装（从构造函数来看就是一个简单的```HashMap```）：

<details>
<summary>HeapBroadcastState</summary>

```java
// HeapBroadcastState.class
public class HeapBroadcastState<K, V> implements BackendWritableBroadcastState<K, V> {

	/**
	 * Meta information of the state, including state name, assignment mode, and serializer.
	 */
	private RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo;

	/**
	 * The internal map the holds the elements of the state.
	 */
	private final Map<K, V> backingMap;

	/**
	 * A serializer that allows to perform deep copies of internal map state.
	 */
	private final MapSerializer<K, V> internalMapCopySerializer;

	HeapBroadcastState(RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo) {
		this(stateMetaInfo, new HashMap<>());
	}

	private HeapBroadcastState(final RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo, final Map<K, V> internalMap) {

		this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
		this.backingMap = Preconditions.checkNotNull(internalMap);
		this.internalMapCopySerializer = new MapSerializer<>(stateMetaInfo.getKeySerializer(), stateMetaInfo.getValueSerializer());
	}

	private HeapBroadcastState(HeapBroadcastState<K, V> toCopy) {
		this(toCopy.stateMetaInfo.deepCopy(), toCopy.internalMapCopySerializer.copy(toCopy.backingMap));
	}

	@Override
	public void setStateMetaInfo(RegisteredBroadcastStateBackendMetaInfo<K, V> stateMetaInfo) {
		this.stateMetaInfo = stateMetaInfo;
	}

	@Override
	public RegisteredBroadcastStateBackendMetaInfo<K, V> getStateMetaInfo() {
		return stateMetaInfo;
	}

	@Override
	public HeapBroadcastState<K, V> deepCopy() {
		return new HeapBroadcastState<>(this);
	}

	@Override
	public void clear() {
		backingMap.clear();
	}

	@Override
	public String toString() {
		return "HeapBroadcastState{" +
				"stateMetaInfo=" + stateMetaInfo +
				", backingMap=" + backingMap +
				", internalMapCopySerializer=" + internalMapCopySerializer +
				'}';
	}

	@Override
	public long write(FSDataOutputStream out) throws IOException {
		long partitionOffset = out.getPos();

		DataOutputView dov = new DataOutputViewStreamWrapper(out);
		dov.writeInt(backingMap.size());
		for (Map.Entry<K, V> entry: backingMap.entrySet()) {
			getStateMetaInfo().getKeySerializer().serialize(entry.getKey(), dov);
			getStateMetaInfo().getValueSerializer().serialize(entry.getValue(), dov);
		}

		return partitionOffset;
	}

	@Override
	public V get(K key) {
		return backingMap.get(key);
	}

	@Override
	public void put(K key, V value) {
		backingMap.put(key, value);
	}

	@Override
	public void putAll(Map<K, V> map) {
		backingMap.putAll(map);
	}

	@Override
	public void remove(K key) {
		backingMap.remove(key);
	}

	@Override
	public boolean contains(K key) {
		return backingMap.containsKey(key);
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return backingMap.entrySet().iterator();
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() {
		return backingMap.entrySet();
	}

	@Override
	public Iterable<Map.Entry<K, V>> immutableEntries() {
		return Collections.unmodifiableSet(backingMap.entrySet());
	}
}

```
</details>

### Broadcast状态的获取与创建

Broadcast状态通过```OperatorStateBackend#getBroadcastState```方法得到（```BroadcastState```内部是一个HashMap，所以设计上只支持```MapStateDescriptor```来描述）：

```java
// DefaultOperatorStateBackend.class第149行
public <K, V> BroadcastState<K, V> getBroadcastState(final MapStateDescriptor<K, V> stateDescriptor) throws StateMigrationException {

	Preconditions.checkNotNull(stateDescriptor);
	String name = Preconditions.checkNotNull(stateDescriptor.getName());

	BackendWritableBroadcastState<K, V> previous =
		(BackendWritableBroadcastState<K, V>) accessedBroadcastStatesByName.get(name);

	if (previous != null) {
		checkStateNameAndMode(
				previous.getStateMetaInfo().getName(),
				name,
				previous.getStateMetaInfo().getAssignmentMode(),
				OperatorStateHandle.Mode.BROADCAST);
		return previous;
	}

	stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
	TypeSerializer<K> broadcastStateKeySerializer = Preconditions.checkNotNull(stateDescriptor.getKeySerializer());
	TypeSerializer<V> broadcastStateValueSerializer = Preconditions.checkNotNull(stateDescriptor.getValueSerializer());

	BackendWritableBroadcastState<K, V> broadcastState =
		(BackendWritableBroadcastState<K, V>) registeredBroadcastStates.get(name);

	if (broadcastState == null) {
		broadcastState = new HeapBroadcastState<>(
				new RegisteredBroadcastStateBackendMetaInfo<>(
						name,
						OperatorStateHandle.Mode.BROADCAST,
						broadcastStateKeySerializer,
						broadcastStateValueSerializer));
		registeredBroadcastStates.put(name, broadcastState);
	} else {
		// has restored state; check compatibility of new state access

		checkStateNameAndMode(
				broadcastState.getStateMetaInfo().getName(),
				name,
				broadcastState.getStateMetaInfo().getAssignmentMode(),
				OperatorStateHandle.Mode.BROADCAST);

		RegisteredBroadcastStateBackendMetaInfo<K, V> restoredBroadcastStateMetaInfo = broadcastState.getStateMetaInfo();

		// check whether new serializers are incompatible
		TypeSerializerSchemaCompatibility<K> keyCompatibility =
			restoredBroadcastStateMetaInfo.updateKeySerializer(broadcastStateKeySerializer);
		if (keyCompatibility.isIncompatible()) {
			throw new StateMigrationException("The new key typeSerializer for broadcast state must not be incompatible.");
		}

		TypeSerializerSchemaCompatibility<V> valueCompatibility =
			restoredBroadcastStateMetaInfo.updateValueSerializer(broadcastStateValueSerializer);
		if (valueCompatibility.isIncompatible()) {
			throw new StateMigrationException("The new value typeSerializer for broadcast state must not be incompatible.");
		}

		broadcastState.setStateMetaInfo(restoredBroadcastStateMetaInfo);
	}

	accessedBroadcastStatesByName.put(name, broadcastState);
	return broadcastState;
}
```

Broadcast状态的获取步骤包括三个步骤：
1. 检查缓存```accessedBroadcastStatesByName```
2. 检查所有已注册的状态```registeredBroadcastStates```
3. 如果前两步都没有查到对应的状态，则创建一个新的状态并将其注册到```registeredBroadcastStates```中

在返回结果前，将得到的Broadcast状态放入缓存中。

### Broadcast算子

Broadcast状态只有在Broadcast数据流中才能获取，可以通过```DataStream.broadcast(MapStateDescriptor...)```方法将一个普通数据流转换成```BroadcastStream```（注意必须是带```MapStateDescriptor...```参数的```broadcast```方法，无参方法仅仅是将DataStream向下游算子发送数据的方式改为broadcast模式）：

```java
// DataStream.class第439行
public BroadcastStream<T> broadcast(final MapStateDescriptor<?, ?>... broadcastStateDescriptors) {
	Preconditions.checkNotNull(broadcastStateDescriptors);
	final DataStream<T> broadcastStream = setConnectionType(new BroadcastPartitioner<>());
	return new BroadcastStream<>(environment, broadcastStream, broadcastStateDescriptors);
}
```

Broadcast数据流在和普通数据流连接（connect）后得到```BroadcastConnectedStream```，对应```CoBroadcastWithKeyedOperator```（连接keyed数据流）和```CoBroadcastWithNonKeyedOperator```（连接non-keyed数据流）这两种算子。

```java
// DataStream.class第278行
public <R> BroadcastConnectedStream<T, R> connect(BroadcastStream<R> broadcastStream) {
	return new BroadcastConnectedStream<>(
			environment,
			this,
			Preconditions.checkNotNull(broadcastStream),
			broadcastStream.getBroadcastStateDescriptor());
}

// BroadcastConnectedStream.class第153行
public <KS, OUT> SingleOutputStreamOperator<OUT> process(
		final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
		final TypeInformation<OUT> outTypeInfo) {

	Preconditions.checkNotNull(function);
	Preconditions.checkArgument(inputStream1 instanceof KeyedStream,
			"A KeyedBroadcastProcessFunction can only be used on a keyed stream.");

	TwoInputStreamOperator<IN1, IN2, OUT> operator =
			new CoBroadcastWithKeyedOperator<>(clean(function), broadcastStateDescriptors);
	return transform("Co-Process-Broadcast-Keyed", outTypeInfo, operator);
}

// BroadcastConnectedStream.class第202行
public <OUT> SingleOutputStreamOperator<OUT> process(
		final BroadcastProcessFunction<IN1, IN2, OUT> function,
		final TypeInformation<OUT> outTypeInfo) {

	Preconditions.checkNotNull(function);
	Preconditions.checkArgument(!(inputStream1 instanceof KeyedStream),
			"A BroadcastProcessFunction can only be used on a non-keyed stream.");

	TwoInputStreamOperator<IN1, IN2, OUT> operator =
			new CoBroadcastWithNonKeyedOperator<>(clean(function), broadcastStateDescriptors);
	return transform("Co-Process-Broadcast", outTypeInfo, operator);
}
```

<details>
<summary>CoBroadcastWithNonKeyedOperator（很长）</summary>

```java
public class CoBroadcastWithNonKeyedOperator<IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, BroadcastProcessFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = -1869740381935471752L;

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	private long currentWatermark = Long.MIN_VALUE;

	private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

	private transient TimestampedCollector<OUT> collector;

	private transient Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates;

	private transient ReadWriteContextImpl rwContext;

	private transient ReadOnlyContextImpl rContext;

	public CoBroadcastWithNonKeyedOperator(
			final BroadcastProcessFunction<IN1, IN2, OUT> function,
			final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors) {
		super(function);
		this.broadcastStateDescriptors = Preconditions.checkNotNull(broadcastStateDescriptors);
	}

	@Override
	public void open() throws Exception {
		super.open();

		collector = new TimestampedCollector<>(output);

		this.broadcastStates = new HashMap<>(broadcastStateDescriptors.size());
		for (MapStateDescriptor<?, ?> descriptor: broadcastStateDescriptors) {
			broadcastStates.put(descriptor, getOperatorStateBackend().getBroadcastState(descriptor));
		}

		rwContext = new ReadWriteContextImpl(getExecutionConfig(), userFunction, broadcastStates, getProcessingTimeService());
		rContext = new ReadOnlyContextImpl(getExecutionConfig(), userFunction, broadcastStates, getProcessingTimeService());
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		collector.setTimestamp(element);
		rContext.setElement(element);
		userFunction.processElement(element.getValue(), rContext, collector);
		rContext.setElement(null);
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		collector.setTimestamp(element);
		rwContext.setElement(element);
		userFunction.processBroadcastElement(element.getValue(), rwContext, collector);
		rwContext.setElement(null);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		currentWatermark = mark.getTimestamp();
	}

	private class ReadWriteContextImpl extends Context {

		private final ExecutionConfig config;

		private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

		private final ProcessingTimeService timerService;

		private StreamRecord<IN2> element;

		ReadWriteContextImpl(
				final ExecutionConfig executionConfig,
				final BroadcastProcessFunction<IN1, IN2, OUT> function,
				final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
				final ProcessingTimeService timerService) {

			function.super();
			this.config = Preconditions.checkNotNull(executionConfig);
			this.states = Preconditions.checkNotNull(broadcastStates);
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		void setElement(StreamRecord<IN2> e) {
			this.element = e;
		}

		@Override
		public Long timestamp() {
			checkState(element != null);
			return element.getTimestamp();
		}

		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
			Preconditions.checkNotNull(stateDescriptor);

			stateDescriptor.initializeSerializerUnlessSet(config);
			BroadcastState<K, V> state = (BroadcastState<K, V>) states.get(stateDescriptor);
			if (state == null) {
				throw new IllegalArgumentException("The requested state does not exist. " +
						"Check for typos in your state descriptor, or specify the state descriptor " +
						"in the datastream.broadcast(...) call if you forgot to register it.");
			}
			return state;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			checkArgument(outputTag != null, "OutputTag must not be null.");
			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		public long currentProcessingTime() {
			return timerService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}
	}

	private class ReadOnlyContextImpl extends BroadcastProcessFunction<IN1, IN2, OUT>.ReadOnlyContext {

		private final ExecutionConfig config;

		private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

		private final ProcessingTimeService timerService;

		private StreamRecord<IN1> element;

		ReadOnlyContextImpl(
				final ExecutionConfig executionConfig,
				final BroadcastProcessFunction<IN1, IN2, OUT> function,
				final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
				final ProcessingTimeService timerService) {

			function.super();
			this.config = Preconditions.checkNotNull(executionConfig);
			this.states = Preconditions.checkNotNull(broadcastStates);
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		void setElement(StreamRecord<IN1> e) {
			this.element = e;
		}

		@Override
		public Long timestamp() {
			checkState(element != null);
			return element.hasTimestamp() ? element.getTimestamp() : null;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			checkArgument(outputTag != null, "OutputTag must not be null.");
			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		public long currentProcessingTime() {
			return timerService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		public <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
			Preconditions.checkNotNull(stateDescriptor);

			stateDescriptor.initializeSerializerUnlessSet(config);
			ReadOnlyBroadcastState<K, V> state = (ReadOnlyBroadcastState<K, V>) states.get(stateDescriptor);
			if (state == null) {
				throw new IllegalArgumentException("The requested state does not exist. " +
						"Check for typos in your state descriptor, or specify the state descriptor " +
						"in the datastream.broadcast(...) call if you forgot to register it.");
			}
			return state;
		}
	}
}
```
</details>

<details>
<summary>CoBroadcastWithKeyedOperator（很长）</summary>

```java
public class CoBroadcastWithKeyedOperator<KS, IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT>, Triggerable<KS, VoidNamespace> {

	private static final long serialVersionUID = 5926499536290284870L;

	private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

	private transient TimestampedCollector<OUT> collector;

	private transient Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates;

	private transient ReadWriteContextImpl rwContext;

	private transient ReadOnlyContextImpl rContext;

	private transient OnTimerContextImpl onTimerContext;

	public CoBroadcastWithKeyedOperator(
			final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
			final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors) {
		super(function);
		this.broadcastStateDescriptors = Preconditions.checkNotNull(broadcastStateDescriptors);
	}

	@Override
	public void open() throws Exception {
		super.open();

		InternalTimerService<VoidNamespace> internalTimerService =
				getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

		TimerService timerService = new SimpleTimerService(internalTimerService);

		collector = new TimestampedCollector<>(output);

		this.broadcastStates = new HashMap<>(broadcastStateDescriptors.size());
		for (MapStateDescriptor<?, ?> descriptor: broadcastStateDescriptors) {
			broadcastStates.put(descriptor, getOperatorStateBackend().getBroadcastState(descriptor));
		}

		rwContext = new ReadWriteContextImpl(getExecutionConfig(), getKeyedStateBackend(), userFunction, broadcastStates, timerService);
		rContext = new ReadOnlyContextImpl(getExecutionConfig(), userFunction, broadcastStates, timerService);
		onTimerContext = new OnTimerContextImpl(getExecutionConfig(), userFunction, broadcastStates, timerService);
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		collector.setTimestamp(element);
		rContext.setElement(element);
		userFunction.processElement(element.getValue(), rContext, collector);
		rContext.setElement(null);
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		collector.setTimestamp(element);
		rwContext.setElement(element);
		userFunction.processBroadcastElement(element.getValue(), rwContext, collector);
		rwContext.setElement(null);
	}

	@Override
	public void onEventTime(InternalTimer<KS, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		onTimerContext.timeDomain = TimeDomain.EVENT_TIME;
		onTimerContext.timer = timer;
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
		onTimerContext.timer = null;
	}

	@Override
	public void onProcessingTime(InternalTimer<KS, VoidNamespace> timer) throws Exception {
		collector.eraseTimestamp();
		onTimerContext.timeDomain = TimeDomain.PROCESSING_TIME;
		onTimerContext.timer = timer;
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
		onTimerContext.timer = null;
	}

	private class ReadWriteContextImpl
			extends KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>.Context {

		private final ExecutionConfig config;

		private final KeyedStateBackend<KS> keyedStateBackend;

		private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

		private final TimerService timerService;

		private StreamRecord<IN2> element;

		ReadWriteContextImpl (
				final ExecutionConfig executionConfig,
				final KeyedStateBackend<KS> keyedStateBackend,
				final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
				final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
				final TimerService timerService) {

			function.super();
			this.config = Preconditions.checkNotNull(executionConfig);
			this.keyedStateBackend = Preconditions.checkNotNull(keyedStateBackend);
			this.states = Preconditions.checkNotNull(broadcastStates);
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		void setElement(StreamRecord<IN2> e) {
			this.element = e;
		}

		@Override
		public Long timestamp() {
			checkState(element != null);
			return element.getTimestamp();
		}

		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
			Preconditions.checkNotNull(stateDescriptor);

			stateDescriptor.initializeSerializerUnlessSet(config);
			BroadcastState<K, V> state = (BroadcastState<K, V>) states.get(stateDescriptor);
			if (state == null) {
				throw new IllegalArgumentException("The requested state does not exist. " +
						"Check for typos in your state descriptor, or specify the state descriptor " +
						"in the datastream.broadcast(...) call if you forgot to register it.");
			}
			return state;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			checkArgument(outputTag != null, "OutputTag must not be null.");
			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return timerService.currentWatermark();
		}

		@Override
		public <VS, S extends State> void applyToKeyedState(
				final StateDescriptor<S, VS> stateDescriptor,
				final KeyedStateFunction<KS, S> function) throws Exception {

			keyedStateBackend.applyToAllKeys(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					Preconditions.checkNotNull(stateDescriptor),
					Preconditions.checkNotNull(function));
		}
	}

	private class ReadOnlyContextImpl extends ReadOnlyContext {

		private final ExecutionConfig config;

		private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

		private final TimerService timerService;

		private StreamRecord<IN1> element;

		ReadOnlyContextImpl(
				final ExecutionConfig executionConfig,
				final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
				final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
				final TimerService timerService) {

			function.super();
			this.config = Preconditions.checkNotNull(executionConfig);
			this.states = Preconditions.checkNotNull(broadcastStates);
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		void setElement(StreamRecord<IN1> e) {
			this.element = e;
		}

		@Override
		public Long timestamp() {
			checkState(element != null);
			return element.hasTimestamp() ? element.getTimestamp() : null;
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return timerService.currentWatermark();
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			checkArgument(outputTag != null, "OutputTag must not be null.");
			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		public  <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
			Preconditions.checkNotNull(stateDescriptor);

			stateDescriptor.initializeSerializerUnlessSet(config);
			ReadOnlyBroadcastState<K, V> state = (ReadOnlyBroadcastState<K, V>) states.get(stateDescriptor);
			if (state == null) {
				throw new IllegalArgumentException("The requested state does not exist. " +
						"Check for typos in your state descriptor, or specify the state descriptor " +
						"in the datastream.broadcast(...) call if you forgot to register it.");
			}
			return state;
		}

		@Override
		@SuppressWarnings("unchecked")
		public KS getCurrentKey() {
			return (KS) CoBroadcastWithKeyedOperator.this.getCurrentKey();
		}

	}

	private class OnTimerContextImpl extends KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>.OnTimerContext {

		private final ExecutionConfig config;

		private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

		private final TimerService timerService;

		private TimeDomain timeDomain;

		private InternalTimer<KS, VoidNamespace> timer;

		OnTimerContextImpl(
				final ExecutionConfig executionConfig,
				final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
				final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
				final TimerService timerService) {

			function.super();
			this.config = Preconditions.checkNotNull(executionConfig);
			this.states = Preconditions.checkNotNull(broadcastStates);
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		@Override
		public Long timestamp() {
			checkState(timer != null);
			return timer.getTimestamp();
		}

		@Override
		public TimeDomain timeDomain() {
			checkState(timeDomain != null);
			return timeDomain;
		}

		@Override
		public KS getCurrentKey() {
			return timer.getKey();
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return timerService.currentWatermark();
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			checkArgument(outputTag != null, "OutputTag must not be null.");
			output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
		}

		@Override
		public <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
			Preconditions.checkNotNull(stateDescriptor);

			stateDescriptor.initializeSerializerUnlessSet(config);
			ReadOnlyBroadcastState<K, V> state = (ReadOnlyBroadcastState<K, V>) states.get(stateDescriptor);
			if (state == null) {
				throw new IllegalArgumentException("The requested state does not exist. " +
						"Check for typos in your state descriptor, or specify the state descriptor " +
						"in the datastream.broadcast(...) call if you forgot to register it.");
			}
			return state;
		}
	}
}
```
</details>

Broadcast算子给广播流处理方法提供了读写上下文（```rwContext```），可以根据广播流获取和更新broadcast状态的内容；给数据流处理方法提供了只读上下文，只能获取broadcast状态的内容（keyed数据流中可以正常获取和更新keyed状态）。必须要注意的是，根据Broadcast算子的open方法，在启动或恢复Broadcast算子时会从算子状态后端中获取```DataStream.broadcast(MapStateDescriptor...)```中定义的状态并加载到自身的类变量中，在算子使用过程中只能访问这些定义过的状态。

## List状态

even-split模式和union模式对应的算子状态都是List状态。关于List State，在OperatorStateStore接口中的描述如下（```getListState(ListStateDescriptor)```和```getUnionListState(ListStateDescriptor)```方法）：

> Creates (or restores) a list state. Each state is registered under a unique name. The provided serializer is used to de/serialize the state in case of checkpointing (snapshot/restore).
>
> Note the semantic differences between an operator list state and a keyed list state (see {@link KeyedStateStore#getListState(ListStateDescriptor)}). Under the context of operator state, the list is a collection of state items that are independent from each other and eligible for redistribution across operator instances in case of changed operator parallelism. In other words, these state items are the finest granularity at which non-keyed state can be redistributed, and should not be correlated with each other.

### List状态的实现

算子状态中的List状态的实现类是```PartitionableListState```。和```HeapBroadcastState```类似，其内部实现相当简单，使用一个ArrayList实现列表状态的存储：

```java
// PartitionableListState.class
public final class PartitionableListState<S> implements ListState<S> {

	/**
	 * Meta information of the state, including state name, assignment mode, and typeSerializer
	 */
	private RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo;

	/**
	 * The internal list the holds the elements of the state
	 */
	private final ArrayList<S> internalList;

	/**
	 * A typeSerializer that allows to perform deep copies of internalList
	 */
	private final ArrayListSerializer<S> internalListCopySerializer;

	PartitionableListState(RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo) {
		this(stateMetaInfo, new ArrayList<S>());
	}

	private PartitionableListState(
			RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo,
			ArrayList<S> internalList) {

		this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
		this.internalList = Preconditions.checkNotNull(internalList);
		this.internalListCopySerializer = new ArrayListSerializer<>(stateMetaInfo.getPartitionStateSerializer());
	}

	private PartitionableListState(PartitionableListState<S> toCopy) {

		this(toCopy.stateMetaInfo.deepCopy(), toCopy.internalListCopySerializer.copy(toCopy.internalList));
	}

	public void setStateMetaInfo(RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo) {
		this.stateMetaInfo = stateMetaInfo;
	}

	public RegisteredOperatorStateBackendMetaInfo<S> getStateMetaInfo() {
		return stateMetaInfo;
	}

	public PartitionableListState<S> deepCopy() {
		return new PartitionableListState<>(this);
	}

	@Override
	public void clear() {
		internalList.clear();
	}

	@Override
	public Iterable<S> get() {
		return internalList;
	}

	@Override
	public void add(S value) {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
		internalList.add(value);
	}

	@Override
	public String toString() {
		return "PartitionableListState{" +
				"stateMetaInfo=" + stateMetaInfo +
				", internalList=" + internalList +
				'}';
	}

	public long[] write(FSDataOutputStream out) throws IOException {

		long[] partitionOffsets = new long[internalList.size()];

		DataOutputView dov = new DataOutputViewStreamWrapper(out);

		for (int i = 0; i < internalList.size(); ++i) {
			S element = internalList.get(i);
			partitionOffsets[i] = out.getPos();
			getStateMetaInfo().getPartitionStateSerializer().serialize(element, dov);
		}

		return partitionOffsets;
	}

	@Override
	public void update(List<S> values) {
		internalList.clear();

		addAll(values);
	}

	@Override
	public void addAll(List<S> values) {
		if (values != null && !values.isEmpty()) {
			internalList.addAll(values);
		}
	}
}
```

### List状态的获取与创建

无论是even-split模式还是union模式，其创建List状态都使用了```getListState(ListStateDescriptor,OperatorStateHandle.Mode)```方法，只是向方法中第二个参数传入了不同的Mode枚举：

```java
// DefaultOperatorStateBackend.class第242行
private <S> ListState<S> getListState(
		ListStateDescriptor<S> stateDescriptor,
		OperatorStateHandle.Mode mode) throws StateMigrationException {

	Preconditions.checkNotNull(stateDescriptor);
	String name = Preconditions.checkNotNull(stateDescriptor.getName());

	@SuppressWarnings("unchecked")
	PartitionableListState<S> previous = (PartitionableListState<S>) accessedStatesByName.get(name);
	if (previous != null) {
		checkStateNameAndMode(
				previous.getStateMetaInfo().getName(),
				name,
				previous.getStateMetaInfo().getAssignmentMode(),
				mode);
		return previous;
	}

	// end up here if its the first time access after execution for the
	// provided state name; check compatibility of restored state, if any
	// TODO with eager registration in place, these checks should be moved to restore()

	stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
	TypeSerializer<S> partitionStateSerializer = Preconditions.checkNotNull(stateDescriptor.getElementSerializer());

	@SuppressWarnings("unchecked")
	PartitionableListState<S> partitionableListState = (PartitionableListState<S>) registeredOperatorStates.get(name);

	if (null == partitionableListState) {
		// no restored state for the state name; simply create new state holder

		partitionableListState = new PartitionableListState<>(
			new RegisteredOperatorStateBackendMetaInfo<>(
				name,
				partitionStateSerializer,
				mode));

		registeredOperatorStates.put(name, partitionableListState);
	} else {
		// has restored state; check compatibility of new state access

		checkStateNameAndMode(
				partitionableListState.getStateMetaInfo().getName(),
				name,
				partitionableListState.getStateMetaInfo().getAssignmentMode(),
				mode);

		RegisteredOperatorStateBackendMetaInfo<S> restoredPartitionableListStateMetaInfo =
			partitionableListState.getStateMetaInfo();

		// check compatibility to determine if new serializers are incompatible
		TypeSerializer<S> newPartitionStateSerializer = partitionStateSerializer.duplicate();

		TypeSerializerSchemaCompatibility<S> stateCompatibility =
			restoredPartitionableListStateMetaInfo.updatePartitionStateSerializer(newPartitionStateSerializer);
		if (stateCompatibility.isIncompatible()) {
			throw new StateMigrationException("The new state typeSerializer for operator state must not be incompatible.");
		}

		partitionableListState.setStateMetaInfo(restoredPartitionableListStateMetaInfo);
	}

	accessedStatesByName.put(name, partitionableListState);
	return partitionableListState;
}
```

List状态的获取和Broadcast状态的获取步骤完全一致，只有缓存Map和注册状态Map变量不同：
1. 检查缓存```accessedStatesByName```
2. 检查所有已注册的状态```registeredOperatorStates```
3. 如果前两步都没有查到对应的状态，则创建一个新的状态并将其注册到```registeredOperatorStates```中

在返回结果前，将得到的List状态放入缓存中。

### even-split模式

获取或是创建一个even-split模式的List状态可以通过```getListState(ListStateDescriptor)```方法。先来看一下源码中even-split模式的相关注释：

> The redistribution scheme of this list state upon operator rescaling is a round-robin pattern, such that the logical whole state (a concatenation of all the lists of state elements previously managed by each operator before the restore) is evenly divided into as many sublists as there are parallel operators.

在even-split模式下，算子的重规划使用round-robin轮询，原先所有even-split模式的List状态会被划分成若干个子列表然后分配到不同的partition中。

```java
// DefaultOperatorStateBackend.class第213行
public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
	return getListState(stateDescriptor, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
}
```

even-split模式对应的重规划方法是```RoundRobinOperatorStateRepartitioner#repartitionSplitState```，将在状态篇后续的文章中介绍。

### union模式

获取或是创建一个union模式的List状态可以通过```getUnionListState(ListStateDescriptor)```方法。同样来看一下源码中union模式的相关注释：

> The redistribution scheme of this list state upon operator rescaling is a broadcast pattern, such that the logical whole state (a concatenation of all the lists of state elements previously managed by each operator before the restore) is restored to all parallel operators so that each of them will get the union of all state items before the restore.

在union模式下，算子的重规划会将算子所有union模式的List状态全都拼接到一起，然后分配给所有的partition中。

```java
public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
	return getListState(stateDescriptor, OperatorStateHandle.Mode.UNION);
}
```

union模式对应的重规划方法是```RoundRobinOperatorStateRepartitioner#repartitionUnionState```，将在状态篇后续的文章中介绍。

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [流式计算系统系列（4）：状态](https://zhuanlan.zhihu.com/p/119305376)