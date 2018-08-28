package HBase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SimpleRowCounter extends Configured implements Tool {  //计算HBase表中行数的MapReduce程序
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {  //TableMapper设定map的输入类型由TableInputFormat来传递，输入的键为行键，值为扫描的行结果
        public static enum Counters {ROWS}

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.ROWS).increment(1);  //每获得表的一行输入，枚举的计数器加一
        }
    }

    public int run(String[] args) throws Exception {
        if(args.length!=1){
            System.err.println("Usage: SimpleRowCounter <tablename>");
            return -1;
        }
        String tableName=args[0];
        Scan scan=new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());  //运行服务器端任务时，只用每行的第一个单元格填充mapper中的Result对象
        Job job=new Job(getConf(),getClass().getSimpleName());
        job.setJarByClass(getClass());
        TableMapReduceUtil.initTableMapperJob(tableName,scan,RowCounterMapper.class,ImmutableBytesWritable.class,Result.class,job);  //用表名、扫描器、Mapper类、输出键值对的类和作业对象来设置TableMap作业
        job.setNumReduceTasks(0);  //设置reducer数量为0
        job.setOutputFormatClass(NullOutputFormat.class);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode= ToolRunner.run(HBaseConfiguration.create(),new SimpleRowCounter(),args);  //exitCode被赋予上面run()方法中最后job.waitForCompletion()的返回值
        System.exit(exitCode);
    }
}
