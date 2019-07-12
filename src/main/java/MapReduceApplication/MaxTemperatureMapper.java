package MapReduceApplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {  //与解析类NcdcRecordParser配合的mapper类

    enum Temperature {
        MALFORMED
    }

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  //输入是一个键值对，Context实例用于输出内容的写入
        parser.parse(value);  //利用解析类中的方法解析温度数据
        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();
            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        } else if (parser.isMalformedTemperature()) {
            System.err.println("Ignoring possibly corrupt input: " + value);  //输出一行到标准错误流以代表有问题的行
            context.getCounter(Temperature.MALFORMED).increment(1);  //用enum类型的字段作为计数器统计忽略的没有首符号气温字段(+或-)的记录数
        }
    }
}
