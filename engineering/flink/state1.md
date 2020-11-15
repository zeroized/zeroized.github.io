# State(1): 状态的实现(上)
2020/10/26

注：源代码为Flink1.11.0版本

## 相关概念

> While many operations in a dataflow simply look at one individual event at a time (for example an event parser), some operations remember information across multiple events (for example window operators). These operations are called stateful. [1]

当一个算子需要处理多个事件、并需要记住之前处理过的事件的结果时，被称为有状态的算子。在Flink中，状态除了记录过去计算的结果，还是进行容错和故障恢复的关键要素。

### Keyed vs Non-keyed

在Flink中，数据流可以分为keyed数据流和non-keyed数据流，其区别在于：keyed数据流将数据进行了**逻辑**分片，从逻辑上每个key对应的partition只包含该key的数据流。当一个算子的计算资源变化时（DataStream#rescale、DataStream#rebalance等，见[Physical Partitioning](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/#physical-partitioning)），keyed数据流能够保证一个key对应的partition中的数据全都分配到同一个task slot中。因此，keyed数据流在进行资源重分配时，其状态能够随着key一同被迁移到新的task slot中；而non-keyed数据流原本在运行时，上游算子就不知道会发送到哪个下游partition中，因此在重分配状态时，难以进行状态的迁移、合并和拆分（只有重放计算才能保证一定正确）。因此，keyed数据流可以使用Keyed State进行元素级的状态更新（支持所有的State类型），而non-keyed数据流只能依赖算子级的状态Operator State来管理状态（只支持List、Union、Broadcast三种）。

## Keyed State

### 状态的初始化

从获取状态的代码中可以看到，在获取状态时，实际是通过```KeyedStateStore#getxxState```方法获取状态。```KeyedStateStore```由```StreamOperatorStateHandler```初始化，初始化过程位于```AbstractStreamOperator#initializeState```：

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

常用的如```RichMapFunction```等类名带有Rich的UDF类都是```RichFunction```接口的实现，注意```ProcessFunction```类和```KeyedProcessFunction```类也是一个RichFunction。这些UDF可以通过```AbstractRichFunction#getRuntimeContext```获取运行时上下文```StreamingRuntimeContext```，然后从中获取任意一种类型的状态。其实际获取状态的调用栈如下：

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

### Time-To-Live

TtlState是HeapState的一个装饰器，提供了根据时间使状态过期的功能（目前只有Processing Time）。TTL的具体使用方法在官方文档[Working With State](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/state/state.html#state-time-to-live-ttl)中的State Time-To-Live (TTL)章节能够看到，在这里就不复述了。TtlState实际上是用```TtlValue```对HeapState保存的值进行了一次装饰，添加了timestamp属性：

```java
// TtlValue.class
public class TtlValue<T> implements Serializable {
	private static final long serialVersionUID = 5221129704201125020L;

	@Nullable
	private final T userValue;
	private final long lastAccessTimestamp;

	public TtlValue(@Nullable T userValue, long lastAccessTimestamp) {
		this.userValue = userValue;
		this.lastAccessTimestamp = lastAccessTimestamp;
	}

	@Nullable
	public T getUserValue() {
		return userValue;
	}

	public long getLastAccessTimestamp() {
		return lastAccessTimestamp;
	}
}
```

可以看到lastAccessTimestamp是不可变的，每次更新TTL时，都会创建一个新的```TtlValue```替换原有的状态值。

#### TTL状态

TtlState与HeapState创建过程的分支点位于```TtlStateFactory#createStateAndWrapWithTtlIfEnabled```处：当设置了Ttl后，这一方法返回了```TtlStateFactory#createState```的结果：

```java
// TtlStateFactory.class第119行
private IS createState() throws Exception {
	SupplierWithException<IS, Exception> stateFactory = stateFactories.get(stateDesc.getClass());
	if (stateFactory == null) {
		String message = String.format("State %s is not supported by %s",
			stateDesc.getClass(), TtlStateFactory.class);
		throw new FlinkRuntimeException(message);
	}
	IS state = stateFactory.get();
	if (incrementalCleanup != null) {
		incrementalCleanup.setTtlState((AbstractTtlState<K, N, ?, TTLSV, ?>) state);
	}
	return state;
}
```

其中```stateFactory.get()```即对应状态类型的```create```方法，如```ValueState```对应```createValueState()```方法：

```java
// TtlStateFactory.class第134行
private IS createValueState() throws Exception {
	ValueStateDescriptor<TtlValue<SV>> ttlDescriptor = new ValueStateDescriptor<>(
		stateDesc.getName(), new TtlSerializer<>(LongSerializer.INSTANCE, stateDesc.getSerializer()));
	return (IS) new TtlValueState<>(createTtlStateContext(ttlDescriptor));
}
```

创建一个TtlState的构造参数是```TtlStateContext```：

```java
private <OIS extends State, TTLS extends State, V, TTLV> TtlStateContext<OIS, V>
	createTtlStateContext(StateDescriptor<TTLS, TTLV> ttlDescriptor) throws Exception {

	ttlDescriptor.enableTimeToLive(stateDesc.getTtlConfig()); // also used by RocksDB backend for TTL compaction filter config
	OIS originalState = (OIS) stateBackend.createInternalState(
		namespaceSerializer, ttlDescriptor, getSnapshotTransformFactory());
	return new TtlStateContext<>(
		originalState, ttlConfig, timeProvider, (TypeSerializer<V>) stateDesc.getSerializer(),
		registerTtlIncrementalCleanupCallback((InternalKvState<?, ?, ?>) originalState));
}
```

```TtlStateContext```提供了TtlState所需的所有信息：需要装饰的原始HeapState、Ttl配置信息、时间供应器（实际上就是```System::currentTimeMillis```，目前只支持Processing Time）、状态描述的序列化器以及TTL增量清理回调方法。清理回调方法只在获取状态和更新状态前异步执行，对状态提供的功能而言是透明的。此处以ValueState为例，```TtlValueState```由3层结构组成，分别是提供基本TTL功能的```AbstractTtlDecorator```、提供State基本功能的```AbstractTtlState```（namespace、serializer等）、以及提供状态获取、更新功能的```TtlValueState```。

<details>
<summary>TtlValueState</summary>

```java
// TtlValueState.class
class TtlValueState<K, N, T>
	extends AbstractTtlState<K, N, T, TtlValue<T>, InternalValueState<K, N, TtlValue<T>>>
	implements InternalValueState<K, N, T> {
	TtlValueState(TtlStateContext<InternalValueState<K, N, TtlValue<T>>, T> tTtlStateContext) {
		super(tTtlStateContext);
	}

	@Override
	public T value() throws IOException {
		accessCallback.run();
		return getWithTtlCheckAndUpdate(original::value, original::update);
	}

	@Override
	public void update(T value) throws IOException {
		accessCallback.run();
		original.update(wrapWithTs(value));
	}

	@Nullable
	@Override
	public TtlValue<T> getUnexpiredOrNull(@Nonnull TtlValue<T> ttlValue) {
		return expired(ttlValue) ? null : ttlValue;
	}
}
```
</details>

<details>
<summary>AbstractTtlState</summary>

```java
// AbstractTtlState.class
abstract class AbstractTtlState<K, N, SV, TTLSV, S extends InternalKvState<K, N, TTLSV>>
	extends AbstractTtlDecorator<S>
	implements InternalKvState<K, N, SV> {
	private final TypeSerializer<SV> valueSerializer;

	/** This registered callback is to be called whenever state is accessed for read or write. */
	final Runnable accessCallback;

	AbstractTtlState(TtlStateContext<S, SV> ttlStateContext) {
		super(ttlStateContext.original, ttlStateContext.config, ttlStateContext.timeProvider);
		this.valueSerializer = ttlStateContext.valueSerializer;
		this.accessCallback = ttlStateContext.accessCallback;
	}

	<SE extends Throwable, CE extends Throwable, T> T getWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<T>, SE> getter,
		ThrowingConsumer<TtlValue<T>, CE> updater) throws SE, CE {
		return getWithTtlCheckAndUpdate(getter, updater, original::clear);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return original.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return original.getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<SV> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		original.setCurrentNamespace(namespace);
	}

	@Override
	public byte[] getSerializedValue(
		byte[] serializedKeyAndNamespace,
		TypeSerializer<K> safeKeySerializer,
		TypeSerializer<N> safeNamespaceSerializer,
		TypeSerializer<SV> safeValueSerializer) {
		throw new FlinkRuntimeException("Queryable state is not currently supported with TTL.");
	}

	@Override
	public void clear() {
		original.clear();
		accessCallback.run();
	}

	/**
	 * Check if state has expired or not and update it if it has partially expired.
	 *
	 * @return either non expired (possibly updated) state or null if the state has expired.
	 */
	@Nullable
	public abstract TTLSV getUnexpiredOrNull(@Nonnull TTLSV ttlValue);

	@Override
	public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		throw new UnsupportedOperationException();
	}
}
```
</details>

<details>
<summary>AbstractTtlDecorator</summary>

```java
abstract class AbstractTtlDecorator<T> {
	/** Wrapped original state handler. */
	final T original;

	final StateTtlConfig config;

	final TtlTimeProvider timeProvider;

	/** Whether to renew expiration timestamp on state read access. */
	final boolean updateTsOnRead;

	/** Whether to renew expiration timestamp on state read access. */
	final boolean returnExpired;

	/** State value time to live in milliseconds. */
	final long ttl;

	AbstractTtlDecorator(
		T original,
		StateTtlConfig config,
		TtlTimeProvider timeProvider) {
		Preconditions.checkNotNull(original);
		Preconditions.checkNotNull(config);
		Preconditions.checkNotNull(timeProvider);
		this.original = original;
		this.config = config;
		this.timeProvider = timeProvider;
		this.updateTsOnRead = config.getUpdateType() == StateTtlConfig.UpdateType.OnReadAndWrite;
		this.returnExpired = config.getStateVisibility() == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;
		this.ttl = config.getTtl().toMilliseconds();
	}

	<V> V getUnexpired(TtlValue<V> ttlValue) {
		return ttlValue == null || (!returnExpired && expired(ttlValue)) ? null : ttlValue.getUserValue();
	}

	<V> boolean expired(TtlValue<V> ttlValue) {
		return TtlUtils.expired(ttlValue, ttl, timeProvider);
	}

	<V> TtlValue<V> wrapWithTs(V value) {
		return TtlUtils.wrapWithTs(value, timeProvider.currentTimestamp());
	}

	<V> TtlValue<V> rewrapWithNewTs(TtlValue<V> ttlValue) {
		return wrapWithTs(ttlValue.getUserValue());
	}

	<SE extends Throwable, CE extends Throwable, CLE extends Throwable, V> V getWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<V>, SE> getter,
		ThrowingConsumer<TtlValue<V>, CE> updater,
		ThrowingRunnable<CLE> stateClear) throws SE, CE, CLE {
		TtlValue<V> ttlValue = getWrappedWithTtlCheckAndUpdate(getter, updater, stateClear);
		return ttlValue == null ? null : ttlValue.getUserValue();
	}

	<SE extends Throwable, CE extends Throwable, CLE extends Throwable, V> TtlValue<V> getWrappedWithTtlCheckAndUpdate(
		SupplierWithException<TtlValue<V>, SE> getter,
		ThrowingConsumer<TtlValue<V>, CE> updater,
		ThrowingRunnable<CLE> stateClear) throws SE, CE, CLE {
		TtlValue<V> ttlValue = getter.get();
		if (ttlValue == null) {
			return null;
		} else if (expired(ttlValue)) {
			stateClear.run();
			if (!returnExpired) {
				return null;
			}
		} else if (updateTsOnRead) {
			updater.accept(rewrapWithNewTs(ttlValue));
		}
		return ttlValue;
	}
}
```
</details>

获取状态最终通过```AbstractTtlDecorator#getWrappedWithTtlCheckAndUpdate```方法返回状态值。其中可以看到，只要状态过期，就会执行```HeapValueState#clear```方法清除状态的值（如同官方文档中对TTL状态的描述“在获取状态时清除过期状态”），然后根据TTL设置的```ReturnExpiredIfNotCleanedUp```决定是否要返回该过期值。如果设置了```OnReadAndWrite```的TTL刷新策略，则会使用当前时间戳创建一个新的TtlValue并更新到其装饰的```HeapValueState```中。

其他的TtlState的处理方式和```TtlValueState```基本一致：```TtlReducingState```和```TtlAggregatingState```是丢弃整个过期的结果（本身状态只保存了reduce和aggregate的结果）；```TtlListState```是遍历状态中的列表，并将未过期的值重新组成一个新的列表，代替原来的状态列表；```TtlMapState```直接将过期的<key,value>丢弃。

#### 增量清理

增量清理，即通过设置```TtlConfig.Builder```的```cleanupIncrementally(int,boolean)```方法增加的后台状态清理功能。增量清理的实现位于```TtlIncrementalCleanup```类中，由```TtlStateFactory#registerTtlIncrementalCleanupCallback```构造回调函数，在每次访问状态和更新状态时执行。

```java
// TtlStateFactory.class第210行
private Runnable registerTtlIncrementalCleanupCallback(InternalKvState<?, ?, ?> originalState) {
	StateTtlConfig.IncrementalCleanupStrategy config =
		ttlConfig.getCleanupStrategies().getIncrementalCleanupStrategy();
	boolean cleanupConfigured = config != null && incrementalCleanup != null;
	boolean isCleanupActive = cleanupConfigured &&
		isStateIteratorSupported(originalState, incrementalCleanup.getCleanupSize());
	Runnable callback = isCleanupActive ? incrementalCleanup::stateAccessed : () -> { };
	if (isCleanupActive && config.runCleanupForEveryRecord()) {
		stateBackend.registerKeySelectionListener(stub -> callback.run());
	}
	return callback;
}

// TtlIncrementalCleanup.class第55行
void stateAccessed() {
	initIteratorIfNot();
	try {
		runCleanup();
	} catch (Throwable t) {
		throw new FlinkRuntimeException("Failed to incrementally clean up state with TTL", t);
	}
}

private void initIteratorIfNot() {
	if (stateIterator == null || !stateIterator.hasNext()) {
		stateIterator = ttlState.original.getStateIncrementalVisitor(cleanupSize);
	}
}

private void runCleanup() {
	int entryNum = 0;
	Collection<StateEntry<K, N, S>> nextEntries;
	while (
		entryNum < cleanupSize &&
		stateIterator.hasNext() &&
		!(nextEntries = stateIterator.nextEntries()).isEmpty()) {

		for (StateEntry<K, N, S> state : nextEntries) {
			S cleanState = ttlState.getUnexpiredOrNull(state.getState());
			if (cleanState == null) {
				stateIterator.remove(state);
			} else if (cleanState != state.getState()) {
				stateIterator.update(state, cleanState);
			}
		}

		entryNum += nextEntries.size();
	}
}
```

```TtlStateFactory```在注册增量清理回调函数时，将```stateAccessed```方法注册为清理方法：首先从状态的```stateTable```中获取至多```cleanupSize```个状态，然后逐一检查这些状态是否过期，并直接将过期的状态从tateTable中移除。

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [Streaming 102: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
3. [流式计算系统系列（4）：状态](https://zhuanlan.zhihu.com/p/119305376)