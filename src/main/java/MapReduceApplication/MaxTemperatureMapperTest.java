package MapReduceApplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class MaxTemperatureMapperTest {  //关于MaxTemperatureMapper的单元测试，传递一个天气记录作为mapper的输入，检查输出是否是读入的年份和气温,可以放在maven工程的Test文件夹中进行测试
    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                //Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999"); //Temperature ^^^^^

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())  //指定mapper
                .withInput(new LongWritable(0), value)  //指定输入的键和值，由于mapper忽略输入key，因此输入key可以设置为任何值
                .withOutput(new Text("1950"), new IntWritable(-11))  //期望输出的键值对
                .runTest();  //执行测试
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                //Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");  //Temperature^^^^^

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .runTest();
    }

    @Test
    public void parseMalformedTemperature() throws IOException, InterruptedException {
        Text value = new Text("0335999999433181957042302005+37950+139117SAO +0004" +
                //Year ^^^^
                "RJSN V02011359003150070356999999433201957010100005+353");  //Temperature ^^^^^
        Counters counters = new Counters();
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                .withCounters(counters)
                .runTest();
    }
}
