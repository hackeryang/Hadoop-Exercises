package ZooKeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class ActiveKeyValueStore extends ConnectionWatcher {  //存储键值对形式配置项数据的配置服务，ConfigUpdater的辅助类

    private static final Charset CHARSET=Charset.forName("UTF-8");

    public void write(String path,String value) throws InterruptedException, KeeperException {  //将以znode路径为键的配置项和字符串为值的配置数据写入ZooKeeper
        int retries=0;
        int MAX_RETRIES=3;  //最大重试次数
        int RETRY_PERIOD_SECONDS=10;  //每次重试的间隔时间
        while(true){  //由于下面是幂等操作，在与服务器断开连接重连后，可以进行循环重试
            try{
                //确定指定路径是否存在特定znode的对象，如果watch标志为true，则一旦该znode状态改变，关联的Watcher会被触发（例如组成员加入、退出和组被删除的通知），这里没有使用
                Stat stat=zk.exists(path,false);
                if(stat==null){  //如果不存在指定路径上的znode，则在对应路径创建一个不限访问和修改的持久znode，并在znode中写入序列化后的字节数组形式的配置项属性值
                    zk.create(path,value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }else{  //如果指定路径上的znode存在，则在znode中更新序列化后的字节数组形式的配置项属性值，匹配所有同名的znode所有版本
                    zk.setData(path,value.getBytes(CHARSET),-1);
                }
                return;
            }catch(KeeperException.SessionExpiredException e){
                throw e;
            }catch(KeeperException e){
                if(retries++==MAX_RETRIES){  //如果重试次数达到最大次数，则抛出异常
                    throw e;
                }
                TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);  //每次重连并重试操作后都间隔一段时间
            }
        }
    }

    public String read(String path, Watcher watcher) throws InterruptedException,KeeperException{  //读取路径为/config的znode上的配置项属性值
        byte[] data=zk.getData(path,watcher,null/*stat*/);  //stat对象用于将状态信息元数据随着znode数据一起回传，这里不需要元数据则设为null
        return new String(data,CHARSET);  //通过指定的字符编码将字节数组还原为字符串数据返回，因为ZooKeeper中数据会序列化为字节数组后再存储以节省空间和便于网络传输
    }
}
