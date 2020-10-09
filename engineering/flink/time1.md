# 时间(1)-时间与水印

## Event Time, Processing Time, Ingestion Time

在流式计算概念中，时间可以分为事件时间（Event Time）和处理时间（Processing Time），在这两者的基础上又扩展了摄入时间（Ingestion Time）概念。下面我们展开来说这三种不同的时间概念。

### Processing Time

> Processing time refers to the system time of the machine that is executing the respective operation. [Timely Stream Processing]

处理时间是机器对事件执行某种计算操作时的系统时间。Processing Time是事件无关的，不论事件本身是否带有时间戳信息或是事件是否因某些原因延迟到达。当我们设定系统在10:00整触发一次窗口计算后，无论9:59:59的那条数据是否到达了系统，到10:00的瞬间系统就会开始计算，而迟到的数据则会被放到下一个计算窗口中进行。

我们显然能够发现，使用Processing Time机制会导致以下两个问题：
1. 使用Processing Time丢失了数据本身自带的时间信息，导致与数据发生时间相关的计算无法进行（无法准确进行）
2. 由于系统在计算时只计算已到达的数据，当需要进行重新计算时（尤其是因容错导致的重算），难以保证两次计算的结果是一致的（不可重复）

### Event Time

> Event time is the time that each individual event occurred on its producing device. [Timely Stream Processing]

事件时间是每个事件个体发生时的事件。根据Event Time机制的计算规则，当我们设定系统计算10:00前的窗口时，系统应该等待所有的数据都到达系统后再触发计算。这样就解决了上面Processing Time机制带来的两个问题。但这只是一种理想状态，在分布式系统中，下游系统永远无法知道10:00前到底会来多少条数据，是不是还有因延迟没有到达的数据；在实际生产环境中，也不可能一直等下去。因此有了lateness的概念，即设定一个最晚允许到达时间，一旦超过这个时间（本该在上个窗口计算的）数据还未到达，后续不再进行上个窗口的数据计算。

虽然相比Processing Time机制，Event Time机制在处理时间相关数据时具有极大的优势，但其同样带来了性能上的劣势：
1. 使用Event Time需要为每一条数据进行标记，当数据的时间戳信息是需要转换的时候，会产生大量的性能损耗
2. 在允许lateness的场景下，乱序数据频繁的延迟到达会反复触发重算，占用额外的计算资源

### Ingestion Time

摄入时间是指事件/数据进入系统的时间，其本质上是处理时间的变体。使用Ingestion Time机制能够避免在数据计算链路上的ETL过程产生的延迟（导致经过多次计算后发生乱序），保证先到的事件/数据无论在传播过程中发生怎样的延迟，总能具有更早的时间戳。从这种意义上来说Ingestion Time又更像是（先验地认为数据进入系统是有序的）Event Time，比如事件/数据一经产生就发送到系统中，或是事件本身不具有时间戳信息。

## Watermark机制

<!---
## Watermark的生成

### SourceContext

### WatermarkStrategy

#### Punctuated Watermark

#### Heuristic Watermark

## Watermark的传播 

### Output

## Watermark的处理

###
--->

## 参考文献

1. [Timely Stream Processing](https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/timely-stream-processing.html)
2. [流式计算系统系列（2）：时间](https://zhuanlan.zhihu.com/p/103472646)
3. [Streaming 101: The world beyond batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/)
4. [A Practical Approach to Balancing Correctness, Latency, and Cost in MassiveScale, Unbounded, OutofOrder Data Processing](https://research.google.com/pubs/archive/43864.pdf)