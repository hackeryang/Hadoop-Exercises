package HadoopIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class FileDecompressor {  //通过文件后缀名推断需要使用哪种解压codec
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        FileSystem fs = FileSystem.get(URI.create(uri), conf);  //通过给定的URI和配置权限确定要使用的文件系统

        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);  //CompressionCodecFactory提供一种将文件后缀名映射到一个CompressionCodec的方法
        CompressionCodec codec = factory.getCodec(inputPath);  //获取文件路径中的后缀名
        if (codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }

        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());  //一旦找到对应的解压codec，就去除压缩文件后缀名形成输出文件名，getDefaultExtension()用于获得压缩文件的后缀名，例如“.bz2”

        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));  //FileSystem.create()创建输出流
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
