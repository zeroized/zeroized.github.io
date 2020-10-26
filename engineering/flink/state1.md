# State(1): 状态的实现
2020/10/26

注：源代码为Flink1.11.0版本

## 相关概念

> While many operations in a dataflow simply look at one individual event at a time (for example an event parser), some operations remember information across multiple events (for example window operators). These operations are called stateful. [1]

当一个算子需要处理多个事件、并需要记住之前处理过的事件的结果时，被称为有状态的算子。在Flink中，状态除了记录过去计算的结果，还是进行容错和故障恢复的关键要素。

### Keyed vs Non-keyed

在Flink中，数据流可以分为keyed数据流和non-keyed数据流，其区别在于：keyed数据流将数据进行了**逻辑**分片，从逻辑上每个key对应的partition只包含该key的数据流。当一个算子的计算资源变化时（DataStream#rescale、DataStream#rebalance等，见[Physical Partitioning](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/#physical-partitioning)），keyed数据流能够保证一个key对应的partition中的数据全都分配到同一个task slot中。因此，keyed数据流在进行资源重分配时，其状态能够随着key一同被迁移到新的task slot中；而non-keyed数据流原本在运行时，上游算子就不知道会发送到哪个下游partition中，因此在重分配状态时，难以进行状态的迁移、合并和拆分（只有重放计算才能保证一定正确）。因此，keyed数据流可以使用Keyed State进行元素级的状态更新（支持所有的State类型），而non-keyed数据流只能依赖算子级的状态Operator State来管理状态（只支持List、Union、Broadcast三种）。

## Keyed State

### Heap State

在[Window(3): 窗口的状态](/engineering/flink/window3.md)中已经介绍了窗口的状态管理，其底层实现从```AbstractKeyedStateBackend#getOrCreateKeyedState```开始（non-keyed数据流通过```windowAll()```方法生成的窗口是一个keyed窗口），根据环境配置由HeapState或是HeapState的封装TtlState来实现。

### Ttl State

## 参考文献

1. [Stateful Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/stateful-stream-processing.html)
2. [Streaming 102: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
3. [流式计算系统系列（4）：状态](https://zhuanlan.zhihu.com/p/119305376)