package HadoopIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

public class SequenceFileReadDemo {  //读取包含Writable类型键值对的SequenceFile
    public static void main(String[] args) throws IOException {
        String uri=args[0];
        Configuration conf=new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        FileSystem fs=FileSystem.get(URI.create(uri),conf);  //通过给定的URI和配置权限确定要使用的文件系统
        Path path=new Path(uri);

        SequenceFile.Reader reader=null;
        try{
            reader=new SequenceFile.Reader(fs,path,conf);  //创建读取顺序文件的实例
            Writable key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);  //通过getKeyClass()发现SequenceFile中使用的键类型，然后通过ReflectionUtils对象生成键的实例
            Writable value=(Writable)ReflectionUtils.newInstance(reader.getValueClass(),conf);  //通过getKeyClass()发现SequenceFile中使用的值类型，然后通过ReflectionUtils对象生成值的实例
            long position=reader.getPosition();  //读取位置定位到开头
            while(reader.next(key,value)){  //next()方法迭代读取记录，如果键值对成功读取，返回true，如果已读到文件末尾返回false
                String syncSeen=reader.syncSeen()?"*":"";  //如果读到了同步点所在位置，就在显示所读取数据的第一列多打印一个星号
                System.out.printf("[%s%s]\t%s\t%s\n",position,syncSeen,key,value);
                position=reader.getPosition();  //beginning of next record
            }
        }finally{
            IOUtils.closeStream(reader);
        }
    }
}
