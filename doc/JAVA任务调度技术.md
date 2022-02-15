<div style="text-align: center;"><span style="font-size: xxx-large" >JAVA 任务调度技术</span></div>

# 前言

| 技术                                            |  来源   |
|-----------------------------------------------|:-----:|
| java.util.Timer                               | JDK自带 |
| java.util.concurrent.ScheduledExecutorService | JDK自带 |
| Spring Task                                   |       |
| Quartz                                        |       |
| Elastic-Job                                   |       |
| Apache DolphinScheduler                       |       |
| XXL-JOB                                       |       |



## 1. Timer
### 1.1 核心类
java.util.Timer 是JDK自带的任务调度器。实现比较简单。核心类有以下几个类

| 类                     | 功能                          | 说明                                                                                          |
|-----------------------|-----------------------------|---------------------------------------------------------------------------------------------|
| java.util.Timer       | 入口类，新增任务并启动调度器              |                                                                                             |
| java.util.TaskQueue   | 任务队列                        | TimerTask数组保存了待执行的TimerTask对象。近似有序数组，下标越小，nextExecutionTime越接近当前时间                          |
| java.util.TimerThread | 执行任务的线程，继承了java.lang.Thread | 真正执行任务调度的地方。 核心方法为mainLoop()。                                                               |
| java.util.TimerTask   | 抽象类，通过实现该类的抽象方法来实现业务逻辑      | TimerTask中有一个核心属性：nextExecutionTime下一个执行时间点。很重要。当  nextExecutionTime 下一个执行时间点<当前时间，将会获得执行机会 |

### 1.1 平衡二叉堆
Timer中的的队列为优先队列，基本数据结构为平衡二叉堆(balanced binary heap)。因此想要理解Timer，需要对平衡二叉堆进行了解。
详细可以参考 [【Java基础】JAVA中优先队列详解](https://www.cnblogs.com/satire/p/14919342.html)。摘抄如下：

#### 1.1.1 基本结构
平衡二叉堆的定义为：
>任意一个非叶子节点的权值，都不大于其左右子节点的权值

结构示例如下：
![平衡二叉堆的基本接口](./assets/JAVA任务调度技术-平衡二叉堆的基本接口-1644890890679.png)
从图中可以看出，可以通过数组来实现平衡二叉堆。每一个节点的编号，可以使用数组的下标来表示。
>1. 数组的第一个元素为二叉树的根节点，在所有节点中权值最小。
>2. 父子节点之间的关系可以用以下算法表示。这个算法很重要。在新增元素或者删除元素的时候，都需要使用到该算法。
>> leftNo = parentNo * 2+1  
>> rightNo = parentNo * 2+2  
>> parentNo = (nodeNo-1)/2
> 
在优先队列中，一般只使用到新增元素和删除根节点元素，因此只对这两个算法进行介绍

#### 1.1.2 新增元素
![平衡二叉堆新增元素](./assets/JAVA任务调度技术-平衡二叉堆新增元素-1644891408924.png)
步骤如下:

> 1.先在队尾新增一个元素。如果数组长度不够就先扩容。  
> 2.如果有父节点，则与父节点进行对比。如果权值比父节点小，则与父节点交换位置。  
> 3.重复步骤2，直到没有父节点或者比父节点小则完成新增。2~3步一般称作siftUp。  

```java
//siftUp()
private void siftUp(int k, E x) {
    while (k > 0) {
        int parent = (k - 1) >>> 1;//parentNo = (nodeNo-1)/2
        Object e = queue[parent];
        if (comparator.compare(x, (E) e) >= 0)//调用比较器的比较方法
            break;
        queue[k] = e;
        k = parent;
    }
    queue[k] = x;
}
```
通过以上步骤。可以保证所有的父节点权值都小于子节点的权值。   

#### 1.1.3 删除队首元素

![平衡二叉堆删除元素](./assets/JAVA任务调度技术-平衡二叉堆删除元素-1644891999890.png)
步骤如下：
> 1.删除数组的第一个元素。  
> 2.将队尾的元素放置到头部位置，记为一个父节点。  
> 3.通过比较获取子节点中较小的一个，并与父节点比较，如果父节点大于子节点，则交换位子。  
> 4.重复步骤3，直到父节点小于等于子节点或者已经没有子节点，则结束比较。这个过程一般称作siftDown。

```java
//siftDown()
private void siftDown(int k, E x) {
    int half = size >>> 1;
    while (k < half) {
        //首先找到左右孩子中较小的那个，记录到c里，并用child记录其下标
        int child = (k << 1) + 1;//leftNo = parentNo*2+1
        Object c = queue[child];
        int right = child + 1;
        if (right < size &&
            comparator.compare((E) c, (E) queue[right]) > 0)
            c = queue[child = right];
        if (comparator.compare(x, (E) c) <= 0)
            break;
        queue[k] = c;//然后用c取代原来的值
        k = child;
    }
    queue[k] = x;
}
```

以上步骤可以使用下面的在线数据模拟器进行
> [平衡二叉堆数据结构模拟器](https://iacj.github.io/react-datastructer/#/heap)


### 1.2 核心执行逻辑

![](./assets/README-1644819175724.png)



## 2. ScheduledExecutorService

ScheduledExecutorService的核心在于java.util.concurrent.ScheduledThreadPoolExecutor.DelayedWorkQueue。DelayedWorkQueue

| 类                                                                 | 功能     | 说明                 |
|-------------------------------------------------------------------|--------|--------------------|
| java.util.concurrent.ScheduledThreadPoolExecutor                  | 核心执行器  ||
| java.util.concurrent.ScheduledThreadPoolExecutor.DelayedWorkQueue | 延迟阻塞队列 | 任务周期执行的核心方法在这个类中实现 |
| java.util.concurrent.ScheduledThreadPoolExecutor.ScheduledFutureTask | 队列中的对象 | 说明                 |

### 2.1 ScheduledThreadPoolExecutor类

ScheduledThreadPoolExecutor继承了ThreadPoolExecutor类。其构造函数调用了父类的构造函数的时候，传入了DelayedWorkQueue延时阻塞队列。
```java
public ScheduledThreadPoolExecutor(int corePoolSize,ThreadFactory threadFactory) {
        super(corePoolSize, Integer.MAX_VALUE, 0, NANOSECONDS, new DelayedWorkQueue(), threadFactory);
    }
```
核心的实现在于如何从 DelayedWorkQueue 获取一个任务来交给线程池进行处理。
DelayedWorkQueue 类的定义如下
```java
static class DelayedWorkQueue extends AbstractQueue<Runnable>
        implements BlockingQueue<Runnable> {
}
```
因此 DelayedWorkQueue 是一个阻塞队列。拥有阻塞队列的功能。同时又是一个优先队列。
新增元素包含以下两个方法
```java
    //新增任务
    public boolean offer(Runnable x) {
        if (x == null)
            throw new NullPointerException();
        RunnableScheduledFuture<?> e = (RunnableScheduledFuture<?>)x;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = size;
            if (i >= queue.length)
                grow();
            size = i + 1;
            if (i == 0) {
                //第一个元素，放在头部
                queue[0] = e;
                setIndex(e, 0);
            } else {
                //不是第一个元素，需要进行排序
                siftUp(i, e);
            }
            if (queue[0] == e) {
                leader = null;
                available.signal();
            }
        } finally {
            lock.unlock();
        }
        return true;
    }
    //移动和新增
    private void siftUp(int k, RunnableScheduledFuture<?> key) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            RunnableScheduledFuture<?> e = queue[parent];
            if (key.compareTo(e) >= 0)
                break;
            queue[k] = e;
            setIndex(e, k);
            k = parent;
        }
        queue[k] = key;
        setIndex(key, k);
    }
    
```




>DelayQueue 是一个内部依靠 AQS 队列同步器所实现的无界延迟阻塞队列。
> 
>延迟对象需要覆盖 getDelay 与 compareTo 方法，并且要注意 getDelay 的时间单位的统一，以及 compareTo 根据业务逻辑进行合理的比较逻辑重写。
> [3]

>从源码上看PriorityQueue的入列操作并没对所有加入的元素进行优先级排序。仅仅保证数组第一个元素是最小的即可。[4]


# 引用

1. [Spring Job？Quartz？XXL-Job？年轻人才做选择，艿艿全莽~
](https://mp.weixin.qq.com/s?__biz=MzUzMTA2NTU2Ng==&mid=2247490679&idx=1&sn=25374dbdcca95311d41be5d7b7db454d&chksm=fa4963c6cd3eead055bb9cd10cca13224bb35d0f7373a27aa22a55495f71e24b8273a7603314&scene=27#wechat_redirect)
2. [Timer与TimerTask的真正原理&使用介绍](https://blog.csdn.net/xieyuooo/article/details/8607220)
3. [深入 DelayQueue 内部实现](https://www.zybuluo.com/mikumikulch/note/712598)
4. [PriorityQueue详解](https://www.jianshu.com/p/f1fd9b82cb72)
5. [Java优先级队列DelayedWorkQueue原理分析](https://www.jianshu.com/p/587901245c95)
6. [【Java基础】JAVA中优先队列详解](https://www.cnblogs.com/satire/p/14919342.html)