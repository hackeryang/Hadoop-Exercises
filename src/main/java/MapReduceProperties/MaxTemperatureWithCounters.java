package MapReduceProperties;

import MapReduceApplication.MaxTemperatureReducer;
import MapReduceApplication.NcdcRecordParser;
import MapReduceTypes.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MaxTemperatureWithCounters extends Configured implements Tool {  //统计最高气温的作业，包括用计数器统计气温值缺失的记录、不规范的字段和质量代码
    enum Temperature{
        MISSING,
        MALFORMED
    }

    static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable,Text, Text, IntWritable> {
        private NcdcRecordParser parser=new NcdcRecordParser();  //解析NCDC格式的气温纪录，与MaxTemperatureMapperWithCounters配合的解析类，在同工程MapReduceApplication包内

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if(parser.isValidTemperature()){
                int airTemperature=parser.getAirTemperature();
                context.write(new Text(parser.getYear()),new IntWritable(airTemperature));
            }else if(parser.isMalformedTemperature()){
                System.err.println("Ignoring possibly corrupt input: "+value);
                context.getCounter(Temperature.MALFORMED).increment(1);  //如果发现一个不规范字段，不规范计数器加一
            }else if(parser.isMissingTemperature()){
                context.getCounter(Temperature.MISSING).increment(1);  //如果发现一个丢失的温度记录，丢失温度记录的计数器加一
            }
            //dynamic counter
            context.getCounter("TemperatureQuality",parser.getQuality()).increment(1);  //统计对应质量代码的计数器加一，该方法第一个参数为计数器的组名称，第二个参数为计数器名称，组名称会在命令行输出结果进度的时候显示
        }
    }

    public int run(String[] args) throws Exception {
        Job job= JobBuilder.parseInputAndOutput(this,getConf(),args);  //把打印使用说明的逻辑抽取出来并把输入输出路径设定放到这样一个帮助方法中，实现对run()方法的前几行进行简化
        if(job==null){
            return -1;
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MaxTemperatureMapperWithCounters.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode= ToolRunner.run(new MaxTemperatureWithCounters(),args);  //exitCode被赋予上面run()方法中最后job.waitForCompletion()的返回值
        System.exit(exitCode);
    }
}
