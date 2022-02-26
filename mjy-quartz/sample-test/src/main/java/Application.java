import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/2/22 下午6:34
 * @Modified By
 */
public class Application {
    public static void main(String[] args) {
        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            //定义一个工作对象 设置工作名称与组名
            JobDetail job = JobBuilder.newJob(HelloJob.class).withIdentity("job","group1").build();
            JobDetail job2 = JobBuilder.newJob(HelloJob2.class).withIdentity("job2","group1").build();
            //定义一个触发器 简单Trigger 设置工作名称与组名 5秒触发一次
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger1","group1").startNow().withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(5)).build();
            //从以上代码可以知道，Quartz的trigger 和job是分开的，通过group名称进行关联
            //设置工作 与触发器
            scheduler.scheduleJob(job, trigger);
            scheduler.scheduleJob(job2, trigger);
//            scheduler.shutdown();
        } catch (SchedulerException se) {
            se.printStackTrace();
        }
    }
}
