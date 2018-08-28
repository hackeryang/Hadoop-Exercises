package HBase;

import MapReduceApplication.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseTemperatureImporter extends Configured implements Tool {  //从HDFS向observations表导入NCDC气温数据的MapReduce作业
    static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text,K, Put>{
        private NcdcRecordParser parser=new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValidTemperature()){
                byte[] rowKey=RowKeyConverter.makeObservationRowKey(parser.getStationId(),parser.getObservationDate().getTime());  //用观测站ID和观测时间创建HBase表的行键
                Put p=new Put(rowKey);
                p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,HBaseTemperatureQuery.AIRTEMP_QUALIFIER, Bytes.toBytes(parser.getAirTemperature()));  //将有效气温值添加到HBase的observations表的data:airtemp列
                context.write(null,p);  //将map匹配到的输入数据写出到Put对象
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length!=1){
            System.err.println("Usage: HBaseTemperatureImporter <input>");
            return -1;
        }
        Job job=new Job(getConf(),getClass().getSimpleName());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job,new Path(args[0]));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,"observations");  //设置作业输出的HBase表的名称为observations
        job.setMapperClass(HBaseTemperatureMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TableOutputFormat.class);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode= ToolRunner.run(HBaseConfiguration.create(),new HBaseTemperatureImporter(),args);  //exitCode被赋予上面run()方法中最后job.waitForCompletion()的返回值
        System.exit(exitCode);
    }
}
