# 综述篇——从LR到FLEN的进化与演变
2020/06/12

### 本文将会涉及到的模型
LR, LR with POLY2, FM, FFM, FNN, PNN, Wide and Deep Network, DeepFM, DCN

### 正文

让我们从点击率预测任务最早的逻辑回归模型LR（Logistic Regression）开始谈起。

当然，逻辑回归本身也是一种广义的线性模型（线性模型+非线性变换），而且点击率预估任务是一个二分类任务，
所以如果不要求一个二分类结果（如评分），把LR解读为Linear Regression也不是什么大问题。

我们先来看LR模型的数学形式：
$$ y=w_0+\sum_{i=1}^{n}w_ix_i=W^Tx $$