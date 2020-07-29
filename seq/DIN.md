# DIN: Deep Interest Network
2020/07/26

阿里巴巴\[KDD2018\][Deep Interest Network for Click-Through Rate Prediction
](https://arxiv.org/pdf/1706.06978.pdf) (ver. arXiv-v4，同KDD版本)

## 前言

距离DIN正式发布已经过去快两年了（事实上arXiv上的第一版发布于2017年6月21日，距今已经3年了），在此期间阿里巴巴陆陆续续在各家顶会中发布了多个不同类型的推荐模型，DIN也逐步更新到DIEN和DISN（虽然笔者觉得后两者和DIN的结构相似之处颇少）。DIN本质上是一篇工程型的paper：从模型角度上来看，DIN使用了attention池化，在2018年并不是一个创新性很强的模型结构（在2017的v1版本上就提出了类似attention的结构，在后期才改为attention）；而作为非结构部分的模型细节，包括mini-batch aware正则化以及自适应激活函数DICE（类似BN），都透露出一股浓重的实践气息，这才是这篇paper最为出彩的地方。

### 太长不看版结论

优点：使用attention机制强化/削弱用户商品序列中与候选广告相关/不相关的商品，实现对不同的候选广告，用户特征的表示不同，并通过不进行注意力信号值的归一化实现对用户与候选广告之间整体相关性的衡量；提出了在亿级数据量下的训练策略MBA（AUC绝对值比dropout高0.31%）与DICE激活函数（AUC绝对值比PReLU高0.15%）

缺点：使用池化的方式不关心序列内部的顺序，使序列顺序信息在处理过程中丢失（其进化版DIEN使用RNN-based模型解决了这一问题）

## 正文

### 动机

abstract中对动机的描述是这样的：

> In this way (指Embedding&MLP类的模型), user features are compressed into a fixed-length representation vector, in regardless of what candidate ads are. The use of fixed-length vector will be a bottleneck, which brings difficulty for Embedding&MLP methods to capture user’s diverse interests effectively from rich historical behaviors.

这里的描述比较模糊。根据introduction中第3、4段的相关细节描述，笔者对这一问题做一下补充说明：
1. 此处所说的fixed-length representation vector，并不是指如FM、DeepFM这样的dense embedding vector，而是embedding后通过运算（如PNN的product层、NFM的bi-interaction层等）得到的中间结果，如果将串联结构的MLP视作binary分类器的话，那么这个fixed-length vector就是MLP的输入；对于并联结构也是类似。
2. 根据1我们可以导出上述问题的核心所在：即对于同一个user，无论在模型中的candidate是哪一个商品/广告/etc，这个user特征永远是不变的，而这个特征是根据用户的所有交互记录决定的（在模型分析部分有一个比较详细的推论（但不一定十分严谨））。
3. 根据2则可以导出这个问题在商品/广告推荐中导致瓶颈的原因：当我们要向一个女性游泳者推荐护目镜时，我们应当根据她的商品交互记录中的游泳衣，而不是鞋子来进行推荐（例子使用introduction中的原例）。即对于不同的候选商品/广告，一个历史记录序列中的同一个物品应当会发挥不同的作用，这一点是1、2中所描述的fixed-length representation vector所无法完成的。
4. 虽然FNN、PNN、WDL以及其他embedding&MLP模型本身在设计的时候并没有以序列推荐任务作为目标，但从其训练的过程来看，确实具有2、3所描述的问题，这也正是将这些模型直接应用到广告推荐的工业实践中所面临的一大挑战（指用户的兴趣是具有时效性的，将所有的历史记录全部纳入特征的考量范围是不合理的）。

当然还有两个重要的训练技巧也有说明（解决的是超大规模稀疏输入特征问题）：

> Besides, we develop two techniques: mini-batch aware regularization and data adaptive activation function which can help training industrial deep networks with hundreds of millions of parameters.

### 模型结构与pytorch实现

### 模型分析

### 复现相关参数设置