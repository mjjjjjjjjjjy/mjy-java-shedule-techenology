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

java.util.Timer 是JDK自带的任务调度器。实现比较简单。核心类有以下几个类

| 类                     | 功能                          | 说明                                           |
|-----------------------|-----------------------------|----------------------------------------------|
| java.util.Timer       | 入口类，新增任务并启动调度器              |                                              |
| java.util.TaskQueue   | 任务队列                        | 优先队列，头部节点为最早执行的job                           |
| java.util.TimerThread | 执行任务的线程，继承了java.lang.Thread | 核心方法为mainLoop()。                             |
| java.util.TimerTask   | 抽象类，通过实现该类的抽象方法来实现业务逻辑      | TimerTask中有一个核心属性：nextExecutionTime下一个执行时间点。 |

### 1.1 平衡二叉堆
Timer中的的队列为优先队列，基本数据结构为平衡二叉堆(balanced binary heap)。因此想要理解Timer，需要对平衡二叉堆进行了解。
详细可以参考  [【Java基础】JAVA中优先队列详解](https://www.cnblogs.com/satire/p/14919342.html) 。 摘抄如下：

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


### 1.2 Timer 核心执行逻辑
java.util.Timer位于JDK的rt.jar包下。始于jdk1.3。

原始代码如下，只挑选了核心代码展示。
```java
public class Timer {
    // TaskQueue 实现了一个优先队列
    private final TaskQueue queue = new TaskQueue();
    // TimerThread继承了Thread。同时组合了TaskQueue。当Timer实例化时。会启动TimerThread实例的的start()方法。启动线程处理定时任务。
    private final TimerThread thread = new TimerThread(queue);
    
    /**
     * 核心新增定时任务的方法。
     * @param task 为实现了业务的任务类。
     * @param time 为下次执行任务的时间。  
     * @param period  为0时表示不会重复执行。当period !=0时表示会周期性执行
     */
    private void sched(TimerTask task, long time, long period) {
        //以下代码省去了部分校验代码
        synchronized(queue) {
            synchronized(task.lock) {
                task.nextExecutionTime = time;
                task.period = period;
                task.state = TimerTask.SCHEDULED;
            }
            queue.add(task);
            if (queue.getMin() == task)
                //说明加入task之前。队列为空，处于wait状态。需要唤起。
                queue.notify();
        }
    }
    // 通过调整sched方法time的时间，可以实现延迟多久才开始启动一个新任务。 如
    
    //延迟delay毫秒后执行一次任务
    public void schedule(TimerTask task, long delay) {
        sched(task, System.currentTimeMillis()+delay, 0);
    }
     
    //延迟delay毫秒，以固定频率执行定时任务。下次的执行时间为当前系统时间(System.currentTimeMillis())+|period|
    //当发生阻塞时，有可能丢失调度次数
    public void schedule(TimerTask task, long delay, long period) {
        sched(task, System.currentTimeMillis()+delay, -period);
    }
    //延迟delay毫秒，以固定频率执行定时任务。与schedule不同。下次执行时间为当前本应执行时间(nextExecutionTime)+period
    //当发生阻塞时，不会丢失调度次数。
    public void scheduleAtFixedRate(TimerTask task, long delay, long period) {
        sched(task, System.currentTimeMillis()+delay, period);
    }

    /**
     * 构造函数。实际只是启动了TimerThread线程。
     * @param name
     * @param isDaemon
     */
    public Timer(String name, boolean isDaemon) {
        thread.setName(name);
        thread.setDaemon(isDaemon);
        thread.start();
    }
}
```
从以上代码可以看到，当实例化Timer时，将会启动一个TimerThread线程。核心代码如下（省略部分代码）：

```java

class TimerThread extends Thread {
    //标识线程已经启用。当为false时，跳出循环。
    boolean newTasksMayBeScheduled = true;

    private TaskQueue queue;

    public void run() {
        try {
            mainLoop();
        } finally {
            // Someone killed this Thread, behave as if Timer cancelled
            synchronized(queue) {
                newTasksMayBeScheduled = false;
                queue.clear();  // Eliminate obsolete references
            }
        }
    }

    /**
     * The main timer loop.  (See class comment.)
     */
    private void mainLoop() {
        while (true) {
            try {
                TimerTask task;
                boolean taskFired;
                synchronized(queue) {
                    // Wait for queue to become non-empty
                    while (queue.isEmpty() && newTasksMayBeScheduled)
                        //让出锁，等待新任务加入
                        queue.wait();
                    //此时newTasksMayBeScheduled==false 跳出循环。
                    if (queue.isEmpty())
                        break; // Queue is empty and will forever remain; die

                    // Queue nonempty; look at first evt and do the right thing
                    long currentTime, executionTime;
                    task = queue.getMin();
                    synchronized(task.lock) {
                        //判断状态
                        if (task.state == TimerTask.CANCELLED) {
                            queue.removeMin();
                            continue;  // No action required, poll queue again
                        }
                        currentTime = System.currentTimeMillis();
                        //当前task计划的执行时间。
                        executionTime = task.nextExecutionTime;
                        if (taskFired = (executionTime<=currentTime)) {
                            //当前任务的计划执行时间<=当前时间，则允许执行。
                            if (task.period == 0) { // Non-repeating, remove
                                //只执行一次的任务，移除。
                                queue.removeMin();
                                task.state = TimerTask.EXECUTED;
                            } else { // Repeating task, reschedule
                                //修改task的时间为下一个执行时间，并且重新排序。
                                /**
                                 * void rescheduleMin(long newTime) {
                                        queue[1].nextExecutionTime = newTime;
                                        fixDown(1);
                                 }
                                 */
                                queue.rescheduleMin(
                                  task.period<0 ? currentTime   - task.period
                                                : executionTime + task.period);
                            }
                        }
                    }
                    if (!taskFired) // Task hasn't yet fired; wait
                        //最近时间执行的任务还未到时间，需要等待。让出锁。
                        queue.wait(executionTime - currentTime);
                }
                if (taskFired)  // Task fired; run it, holding no locks
                    //执行业务逻辑。这里可以看到是同步执行的。如果业务逻辑耗时较长，会影响后续任务的执行。
                    task.run();
            } catch(InterruptedException e) {
            }
        }
    }
}
```
一个简单的示例：

```java
public class Application {
    public static void main(String[] args) {
        //初始化一个timer对象
        Timer timer = new Timer();
        //创建TimerTask的实例。
        TimerTask myTask = new TimerTask() {
            @Override
            public void run() {
                System.out.println("执行run方法，time="+System.currentTimeMillis()/1000%60+"秒");
            }
        };
        //提交任务，延迟1秒执行，每两秒执行一次
        timer.schedule(myTask,1000,1000*2);
    }
}
```
结果：
>执行run方法，time=25秒  
执行run方法，time=27秒  
执行run方法，time=29秒  
执行run方法，time=31秒  
执行run方法，time=33秒  
执行run方法，time=35秒  
执行run方法，time=37秒  


### 1.3 Timer 调度示意图

![](./assets/README-1644819175724.png)

### 1.4 Timer 调度示意图


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