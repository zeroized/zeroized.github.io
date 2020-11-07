# State(2): 状态的实现（下）
2020/11/07

在[State(1): 状态的实现（上）](/engineering/flink/state1.md)中介绍了keyed状态初始化、创建和获取的实现、以及Time-To-Live机制的工作方式。本篇将研究无论是是keyed数据流还是non-keyed数据流都能使用的算子状态的原理。

注：源代码为Flink1.11.0版本

## Operator State

相比keyed状态，算子状态更重要的作用是容错和重规划，算子状态的模式决定了算子在容错恢复和重规划时的状态分配，共有even-split、union和broadcast三种模式，其中前两个是List State、第三个是Broadcast State。

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

其中，```snapshotState```方法在每次进行checkpoint时会执行；而```initializeState```方法在每次初始化UDF时会执行，包括算子初始化时以及从故障中恢复时。可以通过初始化状态方法提供的参数```FunctionInitializationContext#getOperatorStateStore```获取到算子状态，然后通过```DefaultOperatorStateBackend#getXXXState```（```DefaultOperatorStateBackend```是目前```OperatorStateStore```接口的唯一实现）可以获取到三种模式对应的List（即even-split模式）、Union和Broadcast状态。注意，这三种状态与keyed State无论是结构还是用途都完全不同。

## Broadcast状态

首先我们来看一下```OperatorStateStore```接口中```getBroadcastState()```方法的相关描述：

> Creates (or restores) a {@link BroadcastState broadcast state}. This type of state can only be created to store the state of a {@code BroadcastStream}. Each state is registered under a unique name. The provided serializer is used to de/serialize the state in case of checkpointing (snapshot/restore). The returned broadcast state has {@code key-value} format.
> 
> **CAUTION: the user has to guarantee that all task instances store the same elements in this type of state.**
>
> Each operator instance individually maintains and stores elements in the broadcast state. The fact that the incoming stream is a broadcast one guarantees that all instances see all the elements. Upon recovery or re-scaling, the same state is given to each of the instances. To avoid hotspots, each task reads its previous partition, and if there are more tasks (scale up), then the new instances read from the old instances in a round robin fashion. This is why each instance has to guarantee that it stores the same elements as the rest. If not, upon recovery or rescaling you may have unpredictable redistribution of the partitions, thus unpredictable results.

实际上broadcast是在广播流上游算子向下游算子的partition发送数据时进行的广播，Broadcast状态只能保证所有的partition能够看到广播流中所有的数据，**不能保证每个算子中的Broadcast状态是一定一致的**。但如果我们先验地**假设**算子的所有partition在看到广播流中的元素能够做出相同的状态更新，由于Flink底层使用TCP保证了数据顺序不会发生变化，就可以认为每个算子中的Broadcast状态是完全一致的，因此在源码中警告用户需要保证对广播状态的更新在每个task实例中是完全一致的。

官方文档中描述了Broadcast状态的三个特性：
1. it has a map format,
2. it is only available to specific operators that have as inputs a broadcasted stream and a non-broadcasted one, and
3. such an operator can have multiple broadcast states with different names.

根据上面的接口描述以及下面将要介绍的实现，印证了官方文档中的相关描述。

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

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [流式计算系统系列（4）：状态](https://zhuanlan.zhihu.com/p/119305376)