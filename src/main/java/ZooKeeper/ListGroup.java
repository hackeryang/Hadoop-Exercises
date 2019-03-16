package ZooKeeper;

import org.apache.zookeeper.KeeperException;

import java.util.List;

public class ListGroup extends ConnectionWatcher {  //列出组成员

    public void list(String groupName) throws KeeperException,InterruptedException{
        String path="/"+groupName;

        try{
            //检索一个znode的子节点列表，传入参数为znode的路径和观察标志，如果watch标志为true，则一旦该znode状态改变，关联的Watcher会被触发（例如组成员加入、退出和组被删除的通知），这里没有使用
            List<String> children=zk.getChildren(path,false);
            if(children.isEmpty()){
                System.out.printf("No members in group %s\n",groupName);
                System.exit(1);
            }
            for(String child:children){
                System.out.println(child);
            }
        }catch(KeeperException.NoNodeException e){
            System.out.printf("Group %s does not exist\n",groupName);
            System.exit(1);
        }
    }

    public static void main(String[] args)throws Exception{
        ListGroup listGroup=new ListGroup();  //创建继承Watcher类的类实例，Watcher对象可以接收各种事件的通知信息
        listGroup.connect(args[0]);  //客户端连接到命令行的第一个参数中指定的ZooKeeper服务器
        listGroup.list(args[1]);  //根据命令行第二个参数指定要列出的组名，并列出组内成员
        listGroup.close();
    }
}
