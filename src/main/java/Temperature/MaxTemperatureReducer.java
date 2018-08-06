package Temperature;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {  //输入参数类型要匹配map()函数的输出类型
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue=Integer.MIN_VALUE;
        for(IntWritable value:values){
            maxValue=Math.max(maxValue,value.get());
        }
        context.write(key,new IntWritable(maxValue));
    }
}
