package ZooKeeper;

import org.apache.zookeeper.KeeperException;

import java.util.List;

public class DeleteGroup extends ConnectionWatcher {  //用于删除一个组及其所有成员的程序
    public void delete(String groupName) throws KeeperException,InterruptedException{
        String path="/"+groupName;

        try{
            //检索一个znode的子节点列表，传入参数为znode的路径和观察标志，如果watch标志为true，则一旦该znode状态改变，关联的Watcher会被触发（例如组成员加入、退出和组被删除的通知），这里没有使用
            List<String> children=zk.getChildren(path,false);
            for(String child:children){
                //两个传入参数为节点路径和版本号，输入的版本号与实际存在的znode版本号一致才会删除。版本号是一种乐观锁机制，使客户端能检测出对znode的修改冲突，设置为-1则匹配所有版本都能删除
                zk.delete(path+"/"+child,-1);  //ZooKeeper不支持递归删除操作，所以删除父节点之前必须先删除子节点
            }
            zk.delete(path,-1);  //删除组成员子节点后再删除组父节点
        }catch(KeeperException.NoNodeException e){
            System.out.printf("Group %s does not exist\n",groupName);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception{
        DeleteGroup deleteGroup=new DeleteGroup();  //创建继承Watcher类的类实例，Watcher对象可以接收各种事件的通知信息
        deleteGroup.connect(args[0]);  //客户端连接到命令行的第一个参数中指定的ZooKeeper服务器
        deleteGroup.delete(args[1]);  //删除命令行第二个参数指定的组
        deleteGroup.close();
    }
}
