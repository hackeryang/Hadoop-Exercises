package ZooKeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

public class ConfigWatcher implements Watcher {  //观察ZooKeeper中配置项属性的更新情况，并将其打印到控制台

    private ActiveKeyValueStore store;

    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        store = new ActiveKeyValueStore();  //新建ActiveKeyValueStore对象，它继承ConnectionWatcher类中的方法
        store.connect(hosts);  //调用ConnectionWatcher类中的connect()方法连接指定的ZooKeeper对象
    }

    public void displayConfig() throws InterruptedException, KeeperException {  //打印读取到的配置项属性值
        String value = store.read(ConfigUpdater.PATH, this);  //调用ActiveKeyValueStore类的read()方法，并将ConfigWatcher自身作为Watcher，用于监控配置项属性值的改动并触发打印
        System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);  //打印/config路径下znode的配置项属性值，每被Wathcer触发一次就打印一次
    }

    @Override
    public void process(WatchedEvent event) {  //Watcher类中包含的唯一方法，当znode的数据被更新改动后（NodeDataChanged事件），该方法会被触发，参数是触发Watcher的事件
        if (event.getType() == Event.EventType.NodeDataChanged) {  //当ConfigUpdate类更新znode时，会产生NodeDataChanged事件，从而触发Watcher，即触发process()
            try {
                displayConfig();  //读取并显示配置属性项被改动后的值
            } catch (InterruptedException e) {
                System.err.println("Interrupted. Exiting.");
                Thread.currentThread().interrupt();
            } catch (KeeperException e) {
                System.err.printf("KeeperException: %s. Exiting.\n", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigWatcher configWatcher = new ConfigWatcher(args[0]);  //新建ConfigWatcher对象并连接命令行第一个参数指定的ZooKeeper节点
        configWatcher.displayConfig();

        //通过线程休眠来模拟正在做某种工作，保持线程占用直到该进程被强行终止，进程被终止后，短暂znode会被ZooKeeper删除
        Thread.sleep(Long.MAX_VALUE);
    }
}
