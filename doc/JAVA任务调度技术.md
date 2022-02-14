<div style="text-align: center;"><span style="font-size: xxx-large" >JAVA 任务调度技术</span></div>

# 前言

| 技术                                            | 来源    |     |     |     |
|-----------------------------------------------|-------|-----|-----|-----|
| java.util.Timer                               | JDK自带 |     |     |     |
| java.util.concurrent.ScheduledExecutorService | JDK自带 |     |     |     |
| Spring Task                                   |       |     |     |     |
| Quartz                                        |       |     |     |     |
| Elastic-Job                                   |       |     |     |     |
| Apache DolphinScheduler                       |       |     |     |     |
| XXL-JOB                                       |       |     |     |     |



## 1. Timer
### 1.1 核心类
java.util.Timer 是JDK自带的任务调度器。实现比较简单。核心类有以下几个类

| 类                     | 功能                          | 说明                                                                                          | 其他  |
|-----------------------|-----------------------------|---------------------------------------------------------------------------------------------|-----|
| java.util.Timer       | 入口类，新增任务并启动调度器              |                                                                                             |     |
| java.util.TaskQueue   | 任务队列                        | TimerTask数组保存了待执行的TimerTask对象。近似有序数组，下标越小，nextExecutionTime越接近当前时间                          |     |
| java.util.TimerThread | 执行任务的线程，继承了java.lang.Thread | 真正执行任务调度的地方。 核心方法为mainLoop()。                                                               |     |
| java.util.TimerTask   | 抽象类，在子类中执行具体的业务逻辑           | TimerTask中有一个核心属性：nextExecutionTime下一个执行时间点。很重要。当  nextExecutionTime 下一个执行时间点<当前时间，将会获得执行机会 |     |

### 1.2 核心执行逻辑

![](./assets/README-1644819175724.png)


# 引用
[Spring Job？Quartz？XXL-Job？年轻人才做选择，艿艿全莽~
](https://mp.weixin.qq.com/s?__biz=MzUzMTA2NTU2Ng==&mid=2247490679&idx=1&sn=25374dbdcca95311d41be5d7b7db454d&chksm=fa4963c6cd3eead055bb9cd10cca13224bb35d0f7373a27aa22a55495f71e24b8273a7603314&scene=27#wechat_redirect)

[Timer与TimerTask的真正原理&使用介绍](https://blog.csdn.net/xieyuooo/article/details/8607220)