package MapReduceProperties;

import HadoopIO.TextPair;
import MapReduceApplication.NcdcRecordParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinRecordMapper extends Mapper<LongWritable, Text, TextPair,Text> {  //在reduce端连接中标记天气数据集的mapper
    private NcdcRecordParser parser=new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        context.write(new TextPair(parser.getStationId(),"1"),value);  //TextPair的第二个字段标记为1以区分它是天气记录
    }
}
