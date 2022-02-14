import java.util.Timer;
import java.util.TimerTask;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/2/13 上午11:23
 * @Modified By
 */
public class ApplicationRepeat {
    public static void main(String[] args) {
        Timer timer = new Timer();

        for (int i = 0; i < 10; i++) {
            timer.schedule(Mytask.get(""+i+"-任务"),1000*i,1000);
        }
    }
}
