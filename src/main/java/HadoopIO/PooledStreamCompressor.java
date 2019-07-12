package HadoopIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

//使用压缩池对从标准输入读取的数据进行压缩，然后将其写到标准输出
public class PooledStreamCompressor {  //使用CodecPool支持反复压缩和解压缩，适用于在应用中执行大量压缩和解压缩操作的场合，可以分摊创建这些对象的开销
    public static void main(String[] args) throws Exception {
        String codecClassname = args[0];
        Class<?> codecClass = Class.forName(codecClassname);  //通过类名字符串获得类对象，用于装载类，要求JVM查找指定的类，并将类加载到内存中，JVM会执行该类的静态代码段
        Configuration conf = new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);  //使用ReflectionUtils新建codec实例
        Compressor compressor = null;
        try {
            compressor = CodecPool.getCompressor(codec);
            CompressionOutputStream out = codec.createOutputStream(System.out, compressor);  //对于指定的CompressionCodec，从压缩池中获取一个Compressor实例
            IOUtils.copyBytes(System.in, out, 4096, false);  //从输入流复制数据，从输出流写入复制的数据，复制缓冲区大小为4096字节，复制结束后不关闭数据流,输出由CompressionOutputStream对象压缩
            out.finish();  //要求压缩方法完成压缩数据流的写操作，但不关闭数据流
        } finally {
            CodecPool.returnCompressor(compressor);  //确保即使出现IOException异常，也可以使compressor可以返回压缩池中
        }
    }
}
