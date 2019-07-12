package HDFS;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

public class URLCat {  //通过URLStreamHandler实例以标准输出方式显示Hadoop文件系统中的文件
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());  //让Java能够识别Hadoop的HDFS URL，用Hadoop的FsUrlStreamHandlerFactory实例调用java.net.URL对象的setURLStreamHandlerFactory()方法，每个JVM只能调用一次这个方法，因此通常在静态方法中调用
    }


    public static void main(String[] args) throws Exception {
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);  //从输入流复制数据，从输出流显示输出数据，复制缓冲区大小为4096字节，复制结束后不关闭数据流
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
