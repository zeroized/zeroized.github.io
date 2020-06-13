# 综述篇——从LR到FLEN的进化与演变
2020/06/12

### 本文将会涉及到的模型
LR, POLY2, FM, FFM, FNN, PNN, Wide and Deep Network, DeepFM, DCN

### 正文

#### 从人工特征交叉到嵌入思想

让我们从点击率预测任务最早的逻辑回归模型LR（Logistic Regression）[1,2]开始谈起。

当然，逻辑回归本身也是一种广义的线性模型（线性模型+非线性变换），而且点击率预估任务是一个二分类任务，
所以如果不要求一个二分类结果（如具体评分），把LR解读为Linear Regression笔者认为也是完全正确的。

我们先来看LR模型的数学形式：

$$ z=w_0+\sum_{i=1}^{n}w_ix_i=W^Tx \tag{1}$$

$$ \hat{y}=\frac{1}{1+e^{-z}} \tag{2}$$

抛开这个sigmoid函数构成的逻辑回归层不看（后面的模型也不会看这个部分），
在（1）式中，权重矩阵$W$反映了样本$x$中每个特征对预测结果的重要程度。
这时候读者要问了，那特征交叉部分呢？在这个阶段，特征交叉都是人工完成的。
人工的特征交叉虽然费时费力，但LR模型给这个过程提供了极大的便利：
因为简单，所以训练速度快；
因为直观，所以看一眼$W$就知道我人工做出来的特征到底好不好。

当然，虽然LR算法也是机器学习中的一个基础算法，但是这整个过程就很“人工学习”，所以LR模型的第二代来了：
带有二阶多项式的LR模型（LR with degree-2 polynomial margin），简称POLY2[3]，其数学形式如下：
$$ z=w_0+\sum_{i=1}^{n}w_ix_i+\sum_{i=1}^{n}{\sum_{j=i+1}^{n}{w_{i,j}x_ix_j}} \tag{3}$$
POLY2提出于一个Linear SVM模型中，解决了特征之间两两组合的自动化问题，在理论模型上看上去十分美好，但他有两个致命缺陷：
- 参数空间大幅度增长：原先$n$个特征对应n个参数，现在$n$个特征对应$\frac{n(n-1)}{2}$个参数
- 二阶多项式参数$w_{i,j}$难以得到训练，极易过拟合：对于高维稀疏的输入样本，实际上能够满足$x_i$和$x_j$都是$1$的特征组合并不多，那么如果长期不出现$x_ix_j=1$，$w_{ij}$就一直没办法更新；而对于少量能够满足$x_ix_j=1$的样本，$w_{ij}$就会一直围绕这几个小样本进行更新

现在终于轮到大名鼎鼎的FM（Factorization Machine）[4]算法出场了。
FM算法应该能算是CTR预估领域的模型里的网红了，隔三岔五被人拿出来技术分享一下，大概是已经被讲烂了。
我们先来看看FM算法的数学形式：
$$ z=w_0+\sum_{i=1}^{n}w_ix_i+\sum_{i=1}^{n}{\sum_{j=i+1}^{n}{\langle v_i,v_j \rangle x_ix_j}} \tag{4}$$
$$ \langle v_i,v_j \rangle =\sum_{l=1}^{k}{v_{i,l}v_{j,l}}$$
简单来说，FM算法把POLY2里的$w_{ij}$拆成了两个定长向量$v_i$和$v_j$的内积，$v_i$和$v_j$分别对应特征$x_i$和$x_j$。
> In this paper, we have introduced factorization machines. FMs bring together the generality of SVMs with the benefits of factorization models.

这是FM算法的conclusion部分的第一句，确实，不论是从SVM（POLY2）来看还是从Factorization来看，FM确实是一个集大成之作。
- 从POLY的角度来看，FM把MF隐因子的思路加进来，做到了解除$w_{ij}$对$x_i$和$x_j$的双重耦合，通过内积$\langle v_i,v_j\rangle$模拟$w_{i,j}$有效地解决了稀疏数据难以收敛、容易过拟合的问题。
- 从Factorization的角度来看，FM泛化了传统的因子分解模型，能够处理0-1二值分类特征以外的连续特征；其对每个特征进行“密集嵌入”的方式也有效地提高了对稀疏用户-物品矩阵的拟合能力。
- FM算法的参数数量仅有$O(kn),k\ll n$，在空间复杂度上比POLY2低了一次；在时间复杂度上，FM能够化简二阶多项式将时间复杂度降低到$O(kn)$，大幅度提高了模型的性能
在点击率预测方向上，FM首次引入了“嵌入”的思想，并引导了一众基于FM嵌入的后续研究（FNN、DeepFM、xDeepFM）。

当然FM后续还跟了一个FFM（Field-aware Factorization Machine）[5]算法，提出了同一特征与不同field的其他特征交叉时使用不同的嵌入向量，其数学形式如下：
$$ z=w_0+\sum_{i=1}^{n}w_ix_i+\sum_{i=1}^{n}{\sum_{j=i+1}^{n}{\langle v_{i,f_j},v_{j,f_i} \rangle x_ix_j}} \tag{5}$$
FFM是FM更为泛化的形式，FM可以看作是所有特征都是同一个field的FFM特例。
FFM固然是一个十分优秀的模型，但其致命缺陷参数量达到了$O(kn^2),k\ll n$，而且二阶多项式不能化简，时间复杂度达到$O(kn^2)$，导致其训练时间大幅长于FM，使得FFM在大部分情况下都仅在理论中出现。



### 参考文献
1. Simple and Scalable Response Prediction for Display Advertising
2. Ad click prediction: a view from the trenches
3. Training and Testing Low-degree Polynomial Data Mappings via Linear SVM
4. Factorization Machines
5. Field-aware Factorization Machines for CTR Prediction 