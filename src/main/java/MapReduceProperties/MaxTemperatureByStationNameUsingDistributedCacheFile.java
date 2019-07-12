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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class MaxTemperatureByStationNameUsingDistributedCacheFile extends Configured implements Tool {  //查找各气象站的最高气温并显示气象站名称，气象站文件是辅助气温主数据集的边数据，存放在HDFS上

    static class StationTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new Text(parser.getStationId()), new IntWritable(parser.getAirTemperature()));  //mapper输出气象站ID和气温数据键值对
            }
        }
    }

    static class MaxTemperatureReducerWithStationLookup extends Reducer<Text, IntWritable, Text, IntWritable> {  //除了查找最高气温，还需要缓存的边数据文件的配合来查找气象站名称

        private NcdcStationMetadata metadata;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            metadata = new NcdcStationMetadata();
            metadata.initialize(new File("station-fixed-width.txt"));  //解析类从HDFS存放的边数据文件中读取并解析出气象站ID与名称保存到解析类内的HashMap中，文件路径与任务的工作目录相同
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String stationName = metadata.getStationName(key.toString());  //将mapper输出的键与解析类NcdcStationMetadata中存储的边数据文件中的键相对照解析出气象站名称（值）

            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(new Text(stationName), new IntWritable(maxValue));  //reducer输出气象站名称和温度最大值键值对
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(StationTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
        System.exit(exitCode);
    }
}
