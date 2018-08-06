package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

public class FileSystemDoubleCat {
    public static void main(String[] args) throws Exception{
        String uri=args[0];
        Configuration conf=new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        FileSystem fs=FileSystem.get(URI.create(uri),conf);  //通过给定的URI和配置权限确定要使用的文件系统
        FSDataInputStream in=null;
        try{
            in=fs.open(new Path(uri));  //FileSystem.open()创建用于读取的输入流
            IOUtils.copyBytes(in,System.out,4096,false);  //从输入流复制数据，从输出流显示输出数据，复制缓冲区大小为4096字节，复制结束后不关闭数据流
            in.seek(0); //go back to the start of the file
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally{
            IOUtils.closeStream(in);
        }
    }
}
