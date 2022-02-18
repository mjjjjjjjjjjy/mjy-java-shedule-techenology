import java.util.concurrent.TimeUnit;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/2/18 下午5:16
 * @Modified By
 */
public class Application {
    public static void main(String[] args) {
        while (true){
            try {
                long l = System.currentTimeMillis() % 1000;
                long timeout = 5000 - l;
                System.out.println("timeout="+timeout+", current second="+l);
                TimeUnit.MILLISECONDS.sleep(timeout);
                System.out.println(System.currentTimeMillis()/1000);
                TimeUnit.MILLISECONDS.sleep(1002);
                System.out.println("执行。。。");
            } catch (InterruptedException e) {
            }
        }
    }
}
