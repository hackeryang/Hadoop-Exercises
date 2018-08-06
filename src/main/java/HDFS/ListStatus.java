package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ListStatus {  //列出指定目录下所有目录与文件
    public static void main(String[] args) throws Exception{
        String uri=args[0];
        Configuration conf=new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        FileSystem fs= FileSystem.get(URI.create(uri),conf);  //通过给定的URI和配置权限确定要使用的文件系统

        Path[] paths=new Path[args.length];
        for(int i=0;i<paths.length;i++){
            paths[i]=new Path(args[i]);
        }
        FileStatus[] status=fs.listStatus(paths);
        Path[] listedPaths= FileUtil.stat2Paths(status);  //stat2Paths方法将一个FileStatus对象数组转换为一个Path对象数组
        for(Path p:listedPaths){
            System.out.println(p);
        }
    }
}
