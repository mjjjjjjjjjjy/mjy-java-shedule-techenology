import java.util.TimerTask;

/**
 * @Author: Mo Jianyue
 * @Description
 * @Date: 2022/2/14 上午11:28
 * @Modified By
 */
public class Mytask extends TimerTask {

    private String name;



    public Mytask(String name) {
        this.name = name;
    }

    public static Mytask get(String name){
        return new Mytask(name);
    }

    @Override
    public void run() {
        System.out.println(name);
    }
}
