package MapReduceTypes;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {  //没有使用键所以为NullWritable，对于大量小文件改进的方法是继承CombineFileInputFormat
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {  //指定输入文件不被分片
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {  //返回定制的RecordReader实现
        WholeFileRecordReader reader=new WholeFileRecordReader();  //依赖于WholeFileRecordReader类
        reader.initialize(split,context);  //初始化输入分片和配置
        return reader;
    }
}
