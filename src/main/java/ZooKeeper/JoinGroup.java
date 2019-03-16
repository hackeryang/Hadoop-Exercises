package ZooKeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

public class JoinGroup extends ConnectionWatcher {  //创建短暂znode作为组成员加入组
    public void join(String groupName,String memberName) throws KeeperException,InterruptedException{
        String path="/"+groupName+"/"+memberName;
        //被创建的znode参数包括所在路径、znode的内容（字节数组，本例中为空值）、访问控制列表（简称ACL，本例中为完全开放ACL，允许任何client对znode读写）、创建znode的类型（临时性znode）
        String createdPath=zk.create(path,null/*data*/, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Created"+createdPath);
    }

    public static void main(String[] args) throws Exception{
        JoinGroup joinGroup=new JoinGroup();  //创建继承Watcher类的类实例，Watcher对象可以接收各种事件的通知信息
        joinGroup.connect(args[0]);  //客户端连接到命令行的第一个参数中指定的ZooKeeper服务器

        joinGroup.join(args[1],args[2]);  //通过命令行的第二个和第三个参数指定znode所属的组名和自己的组成员名，创建该znode并加入特定的路径组

        //通过线程休眠来模拟正在做某种工作，保持线程占用直到该进程被强行终止，进程被终止后，该短暂znode会被ZooKeeper删除
        Thread.sleep(Long.MAX_VALUE);
    }
}
