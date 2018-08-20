package MapReduceProperties;

import HadoopIO.TextPair;
import MapReduceTypes.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinRecordWithStationName extends Configured implements Tool {  //对天气记录和气象站名称进行连接操作

    public static class KeyPartitioner extends Partitioner<TextPair, Text>{
        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;  //利用哈希值作为分区计算函数的一部分，根据组合键的第一个字段（气象站ID）进行分区
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length!=3){
            JobBuilder.printUsage(this,"<ncdc input> <station input> <output>");
            return -1;
        }

        Job job=new Job(getConf(),"Join weather records with station names");  //根据配置文件设置作业，并给作业命名
        job.setJarByClass(getClass());

        Path ncdcInputPath=new Path(args[0]);
        Path stationInputPath=new Path(args[1]);
        Path outputPath=new Path(args[2]);

        MultipleInputs.addInputPath(job,ncdcInputPath, TextInputFormat.class,JoinRecordMapper.class);  //为每个输入指定一个InputFormat和Mapper
        MultipleInputs.addInputPath(job,stationInputPath,TextInputFormat.class,JoinStationMapper.class);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setPartitionerClass(KeyPartitioner.class);  //设置分区类，使同一个键的记录都发往同一个reducer
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);  //设置分组类，分组即把多条相同键的记录作为同一个批次供reduce()函数处理一次，该分组类根据组合键的第一个字段（气象站ID）进行分组

        job.setMapOutputKeyClass(TextPair.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode= ToolRunner.run(new JoinRecordWithStationName(),args);  //exitCode被赋予上面run()方法中最后job.waitForCompletion()的返回值
        System.exit(exitCode);
    }
}
