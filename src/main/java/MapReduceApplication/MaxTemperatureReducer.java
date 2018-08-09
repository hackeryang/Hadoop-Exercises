package MapReduceApplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  //构建IntWritable的迭代器来找到最大值
        int maxValue=Integer.MIN_VALUE;
        for(IntWritable value:values){
            maxValue=Math.max(maxValue,value.get());
        }
        context.write(key,new IntWritable(maxValue));
    }
}
