## 作者简介

笔者于2020年7月毕业，从2015年起接触推荐系统领域，2016年起入坑机器学习和深度学习，研究生阶段主要研究点击率预测任务，对推荐系统领域自认有一定的认知和理解

现工作主要负责流式计算和实时数仓相关开发和优化，因此分布式计算相关理论实践也是笔者（必须）研究的一部分

如果你有任何问题，或是对技术分享的内容有不同意见，欢迎在GitHub上提交Issue进行讨论

[Fork me on GitHb](https://github.com/zeroized/zeroized.github.io)

## 博客简介

### 分布式计算

分布式计算板块是基于当前工作职责和笔者个人兴趣双重驱动更新的，目前以flink相关理论实践为主，包括但不限于框架源代码解析、流式计算原理和工程实践

本版块的内容的主要来源为开源分布式计算框架源代码和分布式计算相关经典paper与理论

### 推荐系统

推荐系统板块是仅基于笔者对推荐系统、机器学习和深度学习等信息检索和AI方向的个人兴趣为动力更新的，因此更新频率不会特别频繁

本板块介绍基于机器学习和深度学习的推荐方法，以点击率预测为主、排序和常规推荐方法为辅，顺带提一些推荐系统中的嵌入方法

笔者将从各类顶会中选择推荐系统领域中的一些文章，对其进行详细的介绍，并使用pytorch将其实现（[DeepRec-torch](https://github.com/zeroized/DeepRec-torch)）

文中大部分论点均来自于paper自身，笔者会在动机和模型分析部分引用paper中的原文进行论述

#### DeepRec-torch

DeepRec-torch是一个基于pytorch的推荐系统模型实现框架，提供了预设的特征工程方法和现有推荐模型的实现，并按照架构分层的思想对训练过程和模型进行了软件工程的设计

开源这个框架的目的更多的是偏向学习而不是工具，希望能够帮助更多初入推荐系统领域的学生/工程师们能够快速对某个推荐算法模型进行实现，从而对模型获得一个大概的认知

## 更新历史

2021/02/22 [Resource Manager(1): Slot的分配与回收](/engineering/flink/resource-manager1.md)

2020/11/22 [Checkpoint(3): 执行与完成checkpoint](/engineering/flink/checkpoint3.md)

2020/11/21 [Checkpoint(2): 处理Barrier](/engineering/flink/checkpoint2.md)

2020/11/16 [Checkpoint(1): 启动Checkpoint](/engineering/flink/checkpoint1.md)

2020/11/08 [State(2): 状态的实现(下)](/engineering/flink/state2.md)

2020/11/07 [State(1): 状态的实现(上)](/engineering/flink/state1.md)

2020/10/19 [Window(3): 窗口的状态](/engineering/flink/window3.md)

2020/10/16 [Window(2): 触发器与回收器](/engineering/flink/window2.md)

2020/10/15 [Window(1): 窗口的分配](/engineering/flink/window1.md)

2020/10/13 [Time & Watermark(2): Watermark的传播与处理](/engineering/flink/time2.md)

2020/10/11 [Time & Watermark(1): 概念与Watermark的生成](/engineering/flink/time1.md)

---
Powered by [docsify](https://docsify.js.org/)

沪ICP备18018236号-1, 点击跳转链接：https://beian.miit.gov.cn
