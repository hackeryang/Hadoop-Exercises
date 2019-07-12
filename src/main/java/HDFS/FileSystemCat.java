package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

public class FileSystemCat {  //直接使用FileSystem以标准输出格式显示Hadoop文件系统中的文件
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        FileSystem fs = FileSystem.get(URI.create(uri), conf);  //通过给定的URI和配置权限确定要使用的文件系统
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));  //open()返回FSDataInputStream对象，不是标准java.io类对象，该类是一个继承了java.io.DataInputStream的一个特殊类，并支持随机访问
            IOUtils.copyBytes(in, System.out, 4096, false);  //从输入流复制数据，从输出流显示输出数据，复制缓冲区大小为4096字节，复制结束后不关闭数据流
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
