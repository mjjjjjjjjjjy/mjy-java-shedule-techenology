import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/3/10 下午10:29
 * @Modified By
 */
public class 测试锁的问题 {

    public static void main(String[] args) {
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(2);
        while (true){
            Scanner scanner = new Scanner(System.in);
            int delay = scanner.nextInt();
            String msg = "执行run方法，提交时间为"+System.currentTimeMillis()/1000%60+"秒，delay="+delay+"秒后执行。";
            Runnable runnable = ()->{System.out.println(msg+"线程名称为"+Thread.currentThread().getName());};
            scheduledExecutorService.schedule(runnable, delay, TimeUnit.SECONDS);
        }
    }
}
