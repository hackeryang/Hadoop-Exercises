package MapReduceApplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class MaxTemperatureReducerTest {  //利用给出的两个温度值测试reducer是否可以筛选并输出最大值，可以放在maven工程的Test文件夹中进行测试
    @Test
    public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())  //指定reducer
                .withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))  //指定输入的键和值
                .withOutput(new Text("1950"), new IntWritable(10))  //期望输出的键值对
                .runTest();  //执行测试
    }
}
