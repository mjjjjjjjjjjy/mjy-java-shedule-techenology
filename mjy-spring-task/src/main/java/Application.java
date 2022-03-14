import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/2/16 下午3:30
 * @Modified By
 */
@Configuration
@EnableScheduling
public class Application {

    public static void main(String[] args) {
        AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(Application.class);
    }
    @Scheduled(fixedRate = 1*1000)
    public void schedled(){
        System.out.println("执行定时任务，time="+System.currentTimeMillis()/1000%60+"秒");
    }

    @Scheduled(fixedDelayString = "${time:1000}")
    public void schedled2(){
        System.out.println("fixedDelayString 执行定时任务，time="+System.currentTimeMillis()/1000%60+"秒");
    }

}
