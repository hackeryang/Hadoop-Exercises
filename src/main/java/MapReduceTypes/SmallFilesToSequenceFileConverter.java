package MapReduceTypes;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SmallFilesToSequenceFileConverter extends Configured implements Tool {
    static class SequenceFileMapper extends Mapper<NullWritable,BytesWritable, Text, BytesWritable> {
        private Text filenameKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split=context.getInputSplit();
            Path path=((FileSplit)split).getPath();  //split原本是Context类型，需要强制转换
            filenameKey=new Text(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(filenameKey,value);
        }
    }

    public int run(String[] args) throws Exception {
        Job job=JobBuilder.parseInputAndOutput(this,getConf(),args);
        if(job==null){
            return -1;
        }

        job.setInputFormatClass(WholeFileInputFormat.class);  //输入格式是自己定制的WholeFileInputFormat，产生的键类型是NullWritable，值类型是BytesWritable
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  //输出格式为sequence file

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(SequenceFileMapper.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode= ToolRunner.run(new SmallFilesToSequenceFileConverter(),args);
        System.exit(exitCode);
    }
}
