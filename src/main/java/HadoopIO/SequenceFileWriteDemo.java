package HadoopIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class SequenceFileWriteDemo {  //将键值对写入一个SequenceFile对象
    private static final String[] DATA={
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void main(String[] args) throws IOException {
        String uri=args[0];
        Configuration conf=new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        FileSystem fs=FileSystem.get(URI.create(uri),conf);  //通过给定的URI和配置权限确定要使用的文件系统
        Path path=new Path(uri);

        IntWritable key=new IntWritable();
        Text value=new Text();
        SequenceFile.Writer writer=null;
        try{
            writer=SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());  //创建SequenceFile对象，并返回SequenceFile.Writer实例
            for(int i=0;i<100;i++){
                key.set(100-i);
                value.set(DATA[i%DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n",writer.getLength(),key,value);  //输出打印已写入长度（即文件当前位置）和键值对
                writer.append(key,value);  //在文件末尾附加键值对
            }
        }finally{
            IOUtils.closeStream(writer);  //关闭数据流
        }
    }
}
