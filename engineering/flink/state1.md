# State(1): 状态的实现
2020/10/26

注：源代码为Flink1.11.0版本

## 相关概念

> While many operations in a dataflow simply look at one individual event at a time (for example an event parser), some operations remember information across multiple events (for example window operators). These operations are called stateful. [1]

当一个算子需要处理多个事件、并需要记住之前处理过的事件的结果时，被称为有状态的算子。在Flink中，状态除了记录过去计算的结果，还是进行容错和故障恢复的关键要素。

### Keyed vs Non-keyed

在Flink中，数据流可以分为keyed数据流和non-keyed数据流，其区别在于：keyed数据流将数据进行了**逻辑**分片，从逻辑上每个key对应的partition只包含该key的数据流。当一个算子的计算资源变化时（DataStream#rescale、DataStream#rebalance等，见[Physical Partitioning](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/#physical-partitioning)），keyed数据流能够保证一个key对应的partition中的数据全都分配到同一个task slot中。因此，keyed数据流在进行资源重分配时，其状态能够随着key一同被迁移到新的task slot中；而non-keyed数据流原本在运行时，上游算子就不知道会发送到哪个下游partition中，因此在重分配状态时，难以进行状态的迁移、合并和拆分（只有重放计算才能保证一定正确）。因此，keyed数据流可以使用Keyed State进行元素级的状态更新（支持所有的State类型），而non-keyed数据流只能依赖算子级的状态Operator State来管理状态（只支持List、Union、Broadcast三种）。

## Keyed State

### 状态的初始化

从上一节的获取状态的代码中可以看到，在获取状态时，实际是通过```KeyedStateStore#getxxState```方法获取状态。```KeyedStateStore```由```StreamOperatorStateHandler```初始化，初始化过程位于```AbstractStreamOperator#initializeState```：

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

```StreamOperatorStateHandler```在构造时，会初始化算子状态后端```operatorStateBackend```（上面Keyed vs Non-keyed节中提到的Operator State的实际状态存储部分，会在下一章介绍）、keyed状态后端```keyedStateBackend```和keyed状态存储```keyedStateStore```（对non-keyed数据流，```keyedStateBackend```是```null```，所以```keyedStateStore```也是```null```）：

```java
// StreamOperatorStateHandler.class第77行
public StreamOperatorStateHandler(
		StreamOperatorStateContext context,
		ExecutionConfig executionConfig,
		CloseableRegistry closeableRegistry) {
	this.context = context;
	operatorStateBackend = context.operatorStateBackend();
	keyedStateBackend = context.keyedStateBackend();
	this.closeableRegistry = closeableRegistry;

	if (keyedStateBackend != null) {
		keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, executionConfig);
	}
	else {
		keyedStateStore = null;
	}
}
```

```keyedStateBackend```是实际状态存储的位置，根据配置可以分为Heap后端和Ttl后端（Ttl状态后端在下一节介绍）；```keyedStateStore```被注册到```runtimeContext```中，负责在计算方法中提供状态的创建和获取。

### 获取的状态和创建

要在算子中管理计算状态，算子的计算方法需要实现RichFunction接口，RichFunction提供了获取运行时上下文的方法```getRuntimeContext()```：

```java
// RichFunction.class
public interface RichFunction extends Function {

	void open(Configuration parameters) throws Exception;

	void close() throws Exception;

	RuntimeContext getRuntimeContext();

	IterationRuntimeContext getIterationRuntimeContext();

	void setRuntimeContext(RuntimeContext t);
}
```

常用的如```RichMapFunction```等类名带有Rich的UDF类都是```RichFunction```接口的实现，注意```ProcessFunction```类和```KeyedProcessFunction```类也是一个RichFunction。这些UDF可以通过```AbstractRichFunction#getRuntimeContext```获取运行时上下文```StreamingRuntimeContext```，然后从中获取任意一种类型的状态。以ValueState为例，其实际获取状态的调用栈如下：

- ```StreamingRuntimeContext#getState```
- ```DefaultKeyedStateStore#getState```
- ```DefaultKeyedStateStore#getPartitionedState```
- ```AbstractKeyedStateBackend#getPartitionedState```(```HeapKeyedStateBackend#getPartitionedState```)

```java
// AbstractKeyedStateBackend.class第308行
public <N, S extends State> S getPartitionedState(
		final N namespace,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) throws Exception {

	checkNotNull(namespace, "Namespace");

	if (lastName != null && lastName.equals(stateDescriptor.getName())) {
		lastState.setCurrentNamespace(namespace);
		return (S) lastState;
	}

	InternalKvState<K, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
	if (previous != null) {
		lastState = previous;
		lastState.setCurrentNamespace(namespace);
		lastName = stateDescriptor.getName();
		return (S) previous;
	}

	final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
	final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;

	lastName = stateDescriptor.getName();
	lastState = kvState;
	kvState.setCurrentNamespace(namespace);

	return state;
}
```

在```AbstractKeyedStateBackend#getPartitionedState```中做了大量的缓存工作，首先查询上一次获取状态时的返回值```lastState```，然后查询历史记录中所有获取过的状态```keyValueStatesByName```（是一个以<name,state>为键值对的map）。当两个缓存中都没有查询到所需的状态，则调用```AbstractKeyedStateBackend#getOrCreateKeyedState```创建一个新的状态（这个方法由于会被```StreamOperatorStateHandler```直接调用，其内部又做了一次缓存的检查）：

```java
// AbstractKeyedStateBackend.class第267行
@Override
@SuppressWarnings("unchecked")
public <N, S extends State, V> S getOrCreateKeyedState(
		final TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, V> stateDescriptor) throws Exception {
	checkNotNull(namespaceSerializer, "Namespace serializer");
	checkNotNull(keySerializer, "State key serializer has not been configured in the config. " +
			"This operation cannot use partitioned state.");

	InternalKvState<K, ?, ?> kvState = keyValueStatesByName.get(stateDescriptor.getName());
	if (kvState == null) {
		if (!stateDescriptor.isSerializerInitialized()) {
			stateDescriptor.initializeSerializerUnlessSet(executionConfig);
		}
		kvState = TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
			namespaceSerializer, stateDescriptor, this, ttlTimeProvider);
		keyValueStatesByName.put(stateDescriptor.getName(), kvState);
		publishQueryableStateIfEnabled(stateDescriptor, kvState);
	}
	return (S) kvState;
}
```

状态是通过```TtlStateFactory#createStateAndWrapWithTtlIfEnabled```创建得到的，并在创建完成后，新创建的状态会加入keyed状态后端的缓存中。其中参数```this```对应的是```AbstractKeyedStateBackend```的实现类```HeapKeyedStateBackend```实例。当不启用Ttl时，实际创建状态的过程位于```HeapKeyedStateBackend#createInternalState```方法：

```java
// HeapKeyedStateBackend.class第263行
@Override
@Nonnull
public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
	@Nonnull TypeSerializer<N> namespaceSerializer,
	@Nonnull StateDescriptor<S, SV> stateDesc,
	@Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
	StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
	if (stateFactory == null) {
		String message = String.format("State %s is not supported by %s",
			stateDesc.getClass(), this.getClass());
		throw new FlinkRuntimeException(message);
	}
	StateTable<K, N, SV> stateTable = tryRegisterStateTable(
		namespaceSerializer, stateDesc, getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory));
	return stateFactory.createState(stateDesc, stateTable, getKeySerializer());
}
```

在创建一个新的状态实例前，向状态表中注册一个新的状态，其主要作用是用于快照，这部分将在后续介绍snapshot、checkpoint等相关机制时介绍。需要注意的是，在调用栈中向```AbstractKeyedStateBackend#getPartitionedState```中传入的参数为```VoidNamespace.INSTANCE```，```VoidNamespaceSerializer.INSTANCE```和```stateDescriptor```，这就意味着keyedState是没有namespace的（对比windowState，其namespace标识了窗口）。

### Ttl State

> A time-to-live (TTL) can be assigned to the keyed state of any type. If a TTL is configured and a state value has expired, the stored value will be cleaned up on a best effort basis which is discussed in more detail below.



## Operator State

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [Streaming 102: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
3. [流式计算系统系列（4）：状态](https://zhuanlan.zhihu.com/p/119305376)