package ZooKeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ConfigUpdater {  //随机更新ZooKeeper中的配置项属性值，与ActiveKeyValueStore类配合

    public static final String PATH = "/config";

    private ActiveKeyValueStore store;
    private Random random = new Random();

    public ConfigUpdater(String hosts) throws IOException, InterruptedException {  //新建该对象实例时会自动调用该构造函数
        store = new ActiveKeyValueStore();  //新建ActiveKeyValueStore对象，它继承ConnectionWatcher类中的方法
        store.connect(hosts);  //调用ConnectionWatcher类中的connect()方法连接指定的ZooKeeper对象
    }

    public void run() throws InterruptedException, KeeperException {
        while (true) {  //永远循环，在随机事件以随机值更新路径为/config的znode中存储的配置项属性值
            String value = random.nextInt(100) + "";  //产生0到100内的随机值
            store.write(PATH, value);  //将路径键和随机配置项属性值写入对应znode
            System.out.printf("Set %s to %s\n", PATH, value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));  //随机休眠0到10秒之间
        }
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            try {
                ConfigUpdater configUpdater = new ConfigUpdater(args[0]);  //新建ConfigUpdater对象并连接命令行第一个参数指定的ZooKeeper节点
                configUpdater.run();
            } catch (KeeperException.SessionExpiredException e) {  //会话过期被关闭将无法再重连该会话，只能新创建一个会话
                //start a new session
            } catch (KeeperException e) {
                //already retried, so exit
                e.printStackTrace();
                break;
            }
        }
    }
}
