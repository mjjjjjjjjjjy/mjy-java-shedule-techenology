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
        Timer timer = new Timer();
        TimerTask myTask = new TimerTask() {
            @Override
            public void run() {
                System.out.println("执行run方法");
//                throw new RuntimeException("异常");
            }
        };

        timer.schedule(myTask,1,1000*2);

        TimerTask myTask2 = new TimerTask() {
            @Override
            public void run() {
                System.out.println("执行run方法2");
            }
        };
        timer.schedule(myTask2,1,1000*2);
    }
}
