package MapReduceProperties;

import MapReduceTypes.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {  //调用TotalOrderPartitioner按IntWritable键对顺序文件进行全局排序

    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);  //把打印使用说明的逻辑抽取出来并把输入输出路径设定放到这样一个帮助方法中，实现对run()方法的前几行进行简化
        if (job == null) {
            return -1;
        }

        job.setInputFormatClass(SequenceFileInputFormat.class);  //设置输入格式为SequenceFile
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  //设置输出格式为SequenceFile
        SequenceFileOutputFormat.setCompressOutput(job, true);  //设置对应作业的输出是否压缩为true
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);  //输出以Gzip方式压缩
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);  //默认是RECORD，每条记录压缩，但是以块形式压缩效果更好

        job.setPartitionerClass(TotalOrderPartitioner.class);  //设置用于分区的类

        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);  //以指定采样率均匀从一个数据集中选择样本，这里采样率为0.1，还有最大样本数和最大分区数，任意一个限制条件满足即停止采样

        InputSampler.writePartitionFile(job, sampler);  //创建一个顺序文件来存储定义分区的键
        //Add to DistributedCache
        Configuration conf = job.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);  //为排序作业创建分区
        URI partitionUri = new URI(partitionFile);
        DistributedCache.addCacheFile(partitionUri, job.getConfiguration());  //job.addCacheFile(partitionUri);　CDH的maven依赖中缺少该写法

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner(), args);  //exitCode被赋予上面run()方法中最后job.waitForCompletion()的返回值
        System.exit(exitCode);
    }
}
