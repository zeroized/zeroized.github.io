# 综述篇——从LR到FLEN的进化与演变
完成于2020/06/15

## 正文

### 从人工特征交叉到嵌入思想

让我们从点击率预测任务最早的逻辑回归模型LR（Logistic Regression）[1,2]开始谈起。

当然，逻辑回归本身也是一种广义的线性模型（线性模型+非线性变换），而且点击率预估任务是一个二分类任务，所以如果不要求一个二分类结果（如具体评分），把LR解读为Linear Regression笔者认为也是完全正确的。

我们先来看LR模型的数学形式：

$$ z=w_0+\sum_{i=1}^{n}w_ix_i=W^Tx \tag{1}$$

$$ \hat{y}=\frac{1}{1+e^{-z}} \tag{2}$$

抛开这个sigmoid函数构成的逻辑回归层不看（后面的模型也不会看这个部分），在（1）式中，权重矩阵$W$反映了样本$x$中每个特征对预测结果的重要程度。这时候读者要问了，那特征交叉部分呢？在这个阶段，特征交叉都是人工完成的。人工的特征交叉虽然费时费力，但LR模型给这个过程提供了极大的便利：
- 因为简单，所以训练速度快；
- 因为直观，所以看一眼$W$就知道我人工做出来的特征到底好不好。

当然，虽然LR算法也是机器学习中的一个基础算法，但是这整个过程就很“人工学习”，所以LR模型的第二代来了：带有二阶多项式的LR模型（LR with degree-2 polynomial margin），简称POLY2[3]，其数学形式如下：

$$ z=w_0+\sum_{i=1}^{n}w_ix_i+\sum_{i=1}^{n}{\sum_{j=i+1}^{n}{w_{i,j}x_ix_j}} \tag{3}$$

POLY2提出于一个Linear SVM模型中，解决了特征之间两两组合的自动化问题，在理论模型上看上去十分美好，但他有两个致命缺陷：
- 参数空间大幅度增长：原先$n$个特征对应n个参数，现在$n$个特征对应$\frac{n(n-1)}{2}$个参数
- 二阶多项式参数$w_{i,j}$难以得到训练，极易过拟合：对于高维稀疏的输入样本，实际上能够满足$x_i$和$x_j$都是$1$的特征组合并不多，那么如果长期不出现$x_ix_j=1$，$w_{ij}$就一直没办法更新；而对于少量能够满足$x_ix_j=1$的样本，$w_{ij}$就会一直围绕这几个小样本进行更新

现在终于轮到大名鼎鼎的FM（Factorization Machine）[4]算法出场了。FM算法应该能算是CTR预估领域的模型里的网红了，隔三岔五被人拿出来技术分享一下，大概是已经被讲烂了。我们先来看看FM算法的数学形式：

$$ z=w_0+\sum_{i=1}^{n}w_ix_i+\sum_{i=1}^{n}{\sum_{j=i+1}^{n}{\langle v_i,v_j \rangle x_ix_j}} \tag{4}$$
$$ \langle v_i,v_j \rangle =\sum_{l=1}^{k}{v_{i,l}v_{j,l}} \tag{5}$$

简单来说，FM算法把POLY2里的$w_{ij}$拆成了两个定长向量$v_i$和$v_j$的内积，$v_i$和$v_j$分别对应特征$x_i$和$x_j$。

> In this paper, we have introduced factorization machines. FMs bring together the generality of SVMs with the benefits of factorization models.
> （Factorization Machines结论部分）

不论是从SVM（POLY2）来看还是从Factorization来看，FM确实是一个集大成之作。
- 从POLY的角度来看，FM把MF隐因子的思路加进来，做到了解除$w_{ij}$对$x_i$和$x_j$的双重耦合，通过内积$\langle v_i,v_j\rangle$模拟$w_{i,j}$有效地解决了稀疏数据难以收敛、容易过拟合的问题。
- 从Factorization的角度来看，FM泛化了传统的因子分解模型，能够处理0-1二值分类特征以外的连续特征；其对每个特征进行“密集嵌入”的方式也有效地提高了对稀疏用户-物品矩阵的拟合能力。
- FM算法的参数数量仅有$O(kn),k\ll n$，在空间复杂度上比POLY2低了一次；在时间复杂度上，FM能够化简二阶多项式将时间复杂度降低到$O(kn)$，大幅度提高了模型的性能
- 数据集里没有出现过的特征组合对FM也能够给出$\langle v_i,v_j \rangle$的结果

在特征交叉这个部分，FM首次引入了“嵌入”的思想（来自MF等factorization），并引导了一众基于FM嵌入的后续研究（NFM、FNN、DeepFM）。

当然FM后续还有一个FFM（Field-aware Factorization Machine）[5]算法，提出了同一特征与不同field的其他特征交叉时使用不同的嵌入向量，其数学形式如下：

$$ z=w_0+\sum_{i=1}^{n}w_ix_i+\sum_{i=1}^{n}{\sum_{j=i+1}^{n}{\langle v_{i,f_j},v_{j,f_i} \rangle x_ix_j}} \tag{6}$$

FFM是FM更为泛化的形式，FM可以看作是所有特征都是同一个field的FFM特例。FFM固然是一个十分优秀的模型，但其致命缺陷参数量达到了$O(kn^2),k\ll n$，而且二阶多项式不能化简，时间复杂度达到$O(kn^2)$，导致其训练时间大幅长于FM，使得FFM在大部分情况下都仅在理论中出现。

### 当深度学习初遇特征交叉

2016年注定不是平凡的一年，三大特征交叉的经典深度模型横空出世：FNN（Factorization-machine supported Neural Netowrk）[6]、PNN（Product-based Neural Network）[7]和Wide & Deep Network[8]。这三个网络模型分别代表了深度学习与特征交叉初次结合时的三种方案：
- 预训练交叉->深度（后合并到交叉->深度）
- 交叉->深度
- 交叉&深度

在这个部分开始之前，笔者觉得值得一提的是，深度模型虽然拟合能力很强，但是它**很难学习特征交叉信息**，所以不能**只用深度模型**来做所有的事情。

#### FM深度进化的开端

FNN作为深度学习+特征交叉的早期成果，直接用预训练的FM嵌入向量拼接起来做成输入，然后用一个两层全连接神经网络去捕获高级特征。很显然，这么搞有个明显的问题：利用特征交叉的FM做了embedding，要通过$\langle v_{i,f_j},v_{j,f_i} \rangle$才能反映出交叉完的权重，是一种隐式的信息。而MLP只能学习低级特征构造高级特征的比例，所以FM预训练embedding中的隐式特征交叉信息其实在往深层传递的时候已经丢失了，或者说MLP是没法进一步利用发挥这个隐式特征交叉信息的。

FNN虽然不太有效，但是他给后续的DeepFM和NFM创造了思路：DeepFM[9]把预训练FM直接换成模型内嵌的FM，减少预训练+MLP的分段误差；NFM（Neural Factorization Machines）[10]把二阶多项式的里的嵌入向量内积换成element-wise乘法，在输入MLP时可以显式地保留特征交叉的信息（Bi-Interaction Pooling层）AFM（Attentional Factorization Machines）[11]则没有用传统的全连接深度模型，而是直接使用注意力池化对交叉后的特征进行加权。

NFM和AFM在处理特征交叉的方式是不一样的：NFM把交叉好的特征全部加起来然后放到神经网络里，其学习的目标是特征每个维度对结果的权重（直接求和意味着每一个交叉特征得到的权重是一样的），
深度网络使得交叉特征的维度有更多不同的组合方式；而AFM则关注不同交叉特征之间的重要性，使用Attention对其进行加权，其pair-wise interaction layer中的$p$可以看作是对交叉特征每个维度的权重。

FM算法门庭若市，而FFM则门可罗雀。ONN（Operation-aware Neural Network，也有叫NFFM的）[12]是少有的FFM思想衍生出的模型。
ONN对每一个特征产生多个embedding，一份embedding属于自己，其他每一个embedding对应与不同特征的交叉，然后把embedding和交叉特征一起放到深度模型里去学习。
但从模型的整体角度来看，ONN更像是PNN的一种变体，只是使用FFM对其embedding层和product层进行了改造。

#### 从内积到外积

PNN提出了基于内积（Inner-product）和外积（Outer-product），其对应的数学形式分别为：

$$ InnerProduct=\langle v_i,v_j \rangle=v_i^Tv_j \tag{7}$$
$$ OuterProduct=v_iv_j^T \tag{8}$$

内积的形式和FM所提供的思路是一致的。一个与内积非常贴近的数学形式叫余弦相似度:

$$ \cos{\theta}=\frac{\langle v_i,v_j \rangle}{\|v_i\|\|v_j\|} \tag{9}$$

内积和余弦相似度的差别仅在于是否对向量的模长进行了归一化，那么内积就能够一定程度上反映两个向量的相似程度。我们再看（5）式内积计算的形式，内积同时也是两个向量的element-wise乘法（Hadamard积）以后再把每个维度加起来，那么内积操作不能完成embedding向量中不同维度之间的特征交叉，只能针对整个向量的层面进行特征交叉，称为vector-wise的特征交叉。再来看外积的展开形式：

$$ OuterProduct=v_iv_j^T=\left[
\begin{matrix}
v_{i,1}v_{j,1} & v_{i,1}v_{j,2} & ... & v_{i,1}v_{j,l} \\
v_{i,2}v_{j,1} & v_{i,2}v_{j,2} & ... & v_{i,2}v_{j,l} \\
... & ... & ... & ... \\
v_{i,l}v_{j,1} & v_{i,l}v_{j,2} & ... & v_{i,l}v_{j,l}
\end{matrix}
\right] \tag{10}$$

在这个外积的展开形式里，我们能看到$v_i$和$v_j$的每一个维度都进行了交叉，是一种bit-wise的交叉（笔者觉得应该叫dimension/dim-wise），那么在最后使用MLP进行每个维度的权重计算时，就能产生更加细粒度的结果。笔者认为这一创新给后续的DCN、xDeepFM这些模型都提供了很好的思路，虽然PNN的实现代码是笔者觉得最麻烦的代码之一（一方面很难直接用矩阵计算得出外积结果Batch * D1 * N * N的四维矩阵；另一方面，作者在代码里给出了外积的形式可以是矩阵、向量和数值，但论文里只说明了矩阵，笔者难以从论文中推导出向量和数值是怎么计算出来的）。


#### Wide & Deep结构范式

Wide & Deep Network其实本身并不是一个特别好的模型（但他有谷歌亲爸爸光环加成啊）。需要人工做好embedding放进去，需要手动指定cross要组合的列，有点回到了LR、在深度模型里“人工学习”的味道了。但Wide & Deep Network的贡献不在于本身模型好不好，而是它提出了一种很重要的模型结构范式：Deep和Cross**分开做**。

Deep网络主要是由一个（到2020年还是2层的）全连接神经网络，也就是MLP构成。我们先来看一下谷歌对于深度模型的评价。

> With less feature engineering, deep neural networks can generalize better to unseen feature combinations through low-dimensional dense embeddings learned for the sparse features.
> However,deep neural networks with embeddings can over-generalize and recommend less relevant items when the user-item interactions are sparse and high-rank.
> (Wide & Deep Learning for Recommender Systems摘要)

从这个摘要里大概可以看出来，从方差-偏差角度看，过度泛化就是偏差太大，说明以这种形式学得的低维嵌入特征组合是效果很差的，所以一定要配合Wide部分来进行。

Wide肯定是专门用来做特征交叉了，那特征交叉又有什么优点呢？

> Memorization of feature interactions through a wide set of cross-product feature transformations are effective and interpretable.
> (Wide & Deep Learning for Recommender Systems摘要)

特征交叉提供了一种“记忆”，能够缓解过度泛化的问题。Wide & Deep Network采用的是人工特征交叉（值得注意的是，谷歌并没有考虑FM那样的自适应特征交叉方法，而是可能沿用了早期LR阶段的大量人工特征组合），因此没有办法对从未出现过的特征交叉进行建模和测试，这一点被Deep模型的“过度泛化”所弥补了。Wide和Deep部分在最后输出决策的地方加权融合，采用一种相互“制衡”的方式进行互补。

### 借鉴、融合与发展

事物的发展是前进性与曲折性的统一。（指借鉴别的算法的优点来获得AUC的提升（不是

#### 自我升华

2017年的DeepFM和DCN（Deep & Cross Network）[13]可以说是集多个前辈之长的成果了。

前面提到了DeepFM是FM的进化版，其实DeepFM应该说是FNN+FM+Wide & Deep Network。NFM走了PNN的路子，把FM特征交叉直接接上深度模型；而DeepFM则用了Wide和Deep结构，将FM作为Wide部分的一个升级版，Deep部分维持不变。这种结构看上去不过是升级过后的FM & Deep Network，实际上，由于FM能够完成自适应的特征交叉嵌入，所以FM这个部分在BP（Back-Propagation）的时候能够更新优化embedding层，从而使得Deep部分的效果变得更好。这种利用Wide优化Deep的思路是到目前为止笔者觉得做的最好的两个之一（另一个就是集大成者FM），相比其前辈所用的“制衡”的方式要前进了一大步。

DCN作为Wide & Deep Network的正统续作，从PNN那里取了经：把Wide部分改成了一个多次交叉的Cross Network。在Cross Network每一层进行特征交叉时，借鉴了ResNet的思想（见DCN参考文献5）（鉴于这里并不是直接把一阶线性结果放到输出层里计算，笔者就不吐槽这一点了），用上一层的交叉特征与原始特征进行外积交叉，使原始特征信息能够一直传递到最后的输出层。

2018年的xDeepFM[14]虽然名字叫xDeepFM，但是他其实是DCN的进化版。之所以名字带有FM，是因为其特征交叉网络CIN（Compressed Interaction Network）采用了FM系列的vector-wise特征交叉思想，将DCN中的外积换成了element-wise乘法（Hadamard积）。还有一个不同之处是，CIN没有直接把最后一层输出到输出层里进行计算，而是对每个特征交叉层进行了一个Compress操作，把每一层中的交叉向量利用和池化压缩成一个数值，然后再把每一层拼接起来输出到最后的输出层中。

#### 西天取经

AutoInt[15]从Transformer里取到了经，在AFM的基础上改成用Multi-head Self-Attention来做交互：在Attention网络的Key、Query、Value中，把每一个特征的嵌入向量依次作为Query，和其他特征的嵌入向量进行交叉（使用的是内积）得到attention signal，接着就是比较常规的加权求和的attention基本操作了。此外，作者也提到了直接把embedding放到输出层一起计算是借鉴了ResNet的思想，但把一阶线性结果放到输出层应该是从LR流传下来的老传统了，笔者觉得这在写法上有取巧的意味（指参考ResNet思想显得高大上）。

从笔者的角度来看，AutoInt算是一个比较成功的跨界模型案例了。Transformer已经出来那么多年了，到现在才刚有把Transformer移植到目标检测上的成功案例，在其他的移植场景下鲜有特别好的模型成果。

#### 温故而知新

FLEN[16]这次又回归了Wide & Deep的模型结构（说到这里大家已经知道Deep不会讲了）。FLEN在embedding部分采用了field-wise的策略，对每一个域（user，item，context等等）建立一个embedding向量，这个embedding向量是域内部所有embedding的和池化结果。
FLEN在名为Field-wise Bi-interaction的特征交叉部分用了三种不同的特征交叉策略：
- S子模块：对原始输入特征进行交叉（LR模型的一阶部分）
- MF子模块：对域间特征进行交叉，对field-wise特征级别的Bi-intercation，只考虑field之间的交叉（field-wise嵌入向量交叉）
- FM子模块：对域内特征进行交叉，对特征级别的Bi-interaction，本质上是考虑field内每个特征的交叉，但因为field-wise嵌入是每个特征embedding的和池化，所以可以方便地使用FM的化简计算形式进行域内的特征交叉。

目的是为了解决嵌入向量的梯度耦合问题：特征a在FM计算过程中，其嵌入向量和其他特征进行交叉（内积），导致a的嵌入向量在BP时得到的梯度与其他特征的嵌入向量耦合在了一起。FLEN将特征之间的交互变成了域内+域外的交互，缓解了梯度耦合的问题，从形式上可以看作是NFM中Bi-interaction的泛化；另一方面，在element-wise product时作dropout来缓解梯度耦合（DiceFactor部分），也是FLEN的一大创新点（特征交叉终于也吃上了dropout，真香）。

## 小结

从LR开始到FLEN，跨度不大的十几年出现了以vector-wise和dot-wise为核心思路的两种特征交互方式，而落到具体的交叉计算方法更是五花八门，每个模型有自己的特点和长处。在引入“外来文化”方面，Attention及其衍生是比较成功的（其实笔者觉得Attention到哪都能成功），尤其是其根据Key和Query生成权重的方式，其实就是vector-wise特征交叉，与这一块天生契合。

花了3天时间，又查了很多资料，终于是把笔者多年来看过、思考过和复现过的东西汇总成了一篇小综述。为了能让读者产生一些概念性的认识，尽可能地没有放公式，在后续对每个模型的详解中会对文章中提到的公式进行详细的分析。

## 参考文献
1. Simple and Scalable Response Prediction for Display Advertising
2. Ad click prediction: a view from the trenches
3. Training and Testing Low-degree Polynomial Data Mappings via Linear SVM
4. Factorization Machines
5. Field-aware Factorization Machines for CTR Prediction 
6. Deep Learning over Multi-field Categorical Data
7. Product-based Neural Networks for User Response
8. Wide & Deep Learning for Recommender Systems
9. DeepFM: A Factorization-Machine based Neural Network for CTR Prediction
10. Neural Factorization Machines for Sparse Predictive Analytics
11. Attentional Factorization Machines: Learning the Weight of Feature Interactions via Attention Networks
12. Operation-aware Neural Networks for User Response Prediction
13. Deep & Cross Network for Ad Click Predictions
14. xDeepFM: Combining Explicit and Implicit Feature Interactions for Recommender Systems
15. AutoInt: Automatic Feature Interaction Learning via Self-Attentive Neural Networks
16. FLEN: Leveraging Field for Scalable CTR Prediction

## 修改历史
2020/06/16：修改一些格式错误

2020/06/15：完成正文第三章及结语

2020/06/14：完成正文第二章

2020/06/13：完成正文第一章