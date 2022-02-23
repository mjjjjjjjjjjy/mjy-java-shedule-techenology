import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/2/22 下午7:00
 * @Modified By
 */
public class HelloJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        System.out.println("HelloJob执行run方法，time="+System.currentTimeMillis()/1000%60+"秒");
    }
}
