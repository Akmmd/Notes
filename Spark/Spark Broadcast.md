## Broadcast

broadcast 就是将数据从一个节点发送到其他各个节点上去。这样的场景很多，比如 driver 上有一张表，其他
节点上运行的 task 需要 lookup 这张表，那么 driver 可以先把这张表 copy 到这些节点，这样 task 就可以在本地查表了。如何实现一个可靠高效的 broadcast 机制是一个有挑战性的问题。

#### *为什么只能 broadcast 只读的变量?*

这就涉及一致性的问题，如果变量可以被更新，那么一旦变量被某个节点更新，其他节点要不要一块更新?如果多个节点 同时在更新，更新顺序是什么?怎么做同步?还会涉及 fault-tolerance 的问题。为了避免维护数据一致性问题，Spark 目 前只支持 broadcast 只读变量。 

#### *broadcast 到节点而不是 broadcast 到每个 task?*

因为每个 task 是一个线程，而且同在一个进程运行 tasks 都属于同一个 application。因此每个节点(executor)上放一份 就可以被所有 task 共享。
 