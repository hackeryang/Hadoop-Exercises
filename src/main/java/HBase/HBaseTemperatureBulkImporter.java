package HBase;

import MapReduceApplication.NcdcRecordParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseTemperatureBulkImporter extends Configured implements Tool {  //HBase的批量加载，以HFile格式先把数据写入HDFS，再将HFile从HDFS写入到HBase，加快导入速度
    static class HBaseTemperatureMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
        private NcdcRecordParser parser=new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValidTemperature()){
                byte[] rowKey=RowKeyConverter.makeObservationRowKey(parser.getStationId(),parser.getObservationDate().getTime());  //用观测站ID和观测时间创建HBase表的行键
                Put p=new Put(rowKey);
                p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,HBaseTemperatureQuery.AIRTEMP_QUALIFIER, Bytes.toBytes(parser.getAirTemperature()));  //将有效气温值添加到HBase的observations表的data:airtemp列
                context.write(new ImmutableBytesWritable(rowKey),p);  //将map匹配到的输入数据写出到Put对象，map输出的键为行键的不可变字节
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length!=1){
            System.err.println("Usage: HBaseTemperatureBulkImporter <input>");
            return -1;
        }
        Configuration conf= HBaseConfiguration.create(getConf());
        Job job=new Job(conf,getClass().getSimpleName());  //设置作业配置，作业名称为类名
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job,new Path(args[0]));
        Path tmpPath=new Path("/tmp/bulk");  //写入HFile的目录
        FileOutputFormat.setOutputPath(job,tmpPath);
        job.setMapperClass(HBaseTemperatureMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        HTable table=new HTable(conf,"observations");  //设置作业输出的HBase表的名称为observations
        try{
            HFileOutputFormat2.configureIncrementalLoad(job,table);  //设置作业输出的键值对的类，设置作业的OutputFormat类，依据当前表中的Region边界自动设置TotalOrderPartitioner以进行分区，使每个HFile恰好适应一个Region

            if(!job.waitForCompletion(true)){
                return 1;
            }

            LoadIncrementalHFiles loader=new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(tmpPath,table);  //将HDFS上的HFile写入到observations表中
            FileSystem.get(conf).delete(tmpPath,true);  //将HFile写入observations表后，删除HDFS上批量加载的HFile目录/tmp/bulk
            return 0;
        }finally{
            table.close();
        }
    }

    public static void main(String[] args) throws Exception{
        int exitCode= ToolRunner.run(HBaseConfiguration.create(),new HBaseTemperatureBulkImporter(),args);
        System.exit(exitCode);
    }
}
