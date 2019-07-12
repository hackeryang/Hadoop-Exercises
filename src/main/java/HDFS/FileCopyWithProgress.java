package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class FileCopyWithProgress {  //将本地文件复制到HDFS，并显示复制进度
    public static void main(String[] args) throws Exception {
        String localSrc = args[0];
        String dst = args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        FileSystem fs = FileSystem.get(URI.create(dst), conf);  //通过给定的URI和配置权限确定要使用的文件系统
        OutputStream out = fs.create(new Path(dst), new Progressable() {  //命令行中指定的复制输出路径必须是一个文件，FileSystem.create()创建输出流
            public void progress() {
                System.out.print(".");  //复制过程中以打印"."来表示进度
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
