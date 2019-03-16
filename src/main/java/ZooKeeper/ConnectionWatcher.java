package ZooKeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ConnectionWatcher implements Watcher {  //等待与ZooKeeper建立连接的辅助类，用于JoinGroup、ListGroup、DeleteGroup和ActiveKeyValueStore类

    private static final int SESSION_TIMEOUT=5000;  //会话超时时间为5秒钟

    protected ZooKeeper zk;  //用于维护客户端和ZooKeeper服务之间的连接
    //用于阻止使用新建的ZooKeeper对象，除非该ZooKeeper对象已经准备就绪，锁存器(latch)创建时带一个值为1的计数器，用于表示在它释放所有等待线程之前需要发生的事件数
    private CountDownLatch connectedSignal=new CountDownLatch(1);

    public void connect(String hosts) throws IOException,InterruptedException{
        //三个参数分别是ZooKeeper服务的主机地址、以毫秒为单位的会话超时时间、Watcher对象实例（用于接收来自ZooKeeper的回调，以获得各种事件通知）
        zk=new ZooKeeper(hosts,SESSION_TIMEOUT,this);  //ZooKeeper实例创建时会启动一个线程连接到ZooKeeper服务，Watcher类用于获取ZooKeeper对象是否准备就绪的信息
        connectedSignal.await();  //当锁存器的计数器变为0时，该方法的等待线程才结束，意思就是等到client和ZooKeeper服务器连接成功并返回连接成功事件信息后，才松开线程允许使用该ZooKeeper对象
    }

    public void process(WatchedEvent event) {  //Watcher类中包含的唯一方法，当客户端与ZooKeeper服务建立连接后（SyncConnected事件），该方法会被触发，参数是触发Watcher的事件
        if(event.getState()== Event.KeeperState.SyncConnected){  //当接收到一个已连接事件时，递减锁存器的计数器
            connectedSignal.countDown();  //调用一次递减计数器的方法后，上面CountDownLatch计数器的值变为0，则await()方法结束并返回
        }
    }

    public void close() throws InterruptedException{
        zk.close();
    }
}
