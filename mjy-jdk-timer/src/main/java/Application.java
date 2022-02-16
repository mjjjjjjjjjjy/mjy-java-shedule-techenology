import java.util.Timer;
import java.util.TimerTask;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/2/13 上午11:23
 * @Modified By
 */
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
