package Temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private static final int MISSING=9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  //输入是一个键值对，Context实例用于输出内容的写入
        String line=value.toString();  //将包含有一行输入的Text值转换为String类型
        String year=line.substring(15,19);
        int airTemperature;
        if(line.charAt(87)=='+'){
            //parseInt doesn't like leading plus signs
            airTemperature=Integer.parseInt(line.substring(88,92));
        }else{
            airTemperature=Integer.parseInt(line.substring(87,92));
        }
        String quality=line.substring(92,93);
        if(airTemperature!=MISSING && quality.matches("[01459]")){
            context.write(new Text(year),new IntWritable(airTemperature));
        }
    }
}
