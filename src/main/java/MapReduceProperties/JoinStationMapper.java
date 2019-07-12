package MapReduceProperties;

import HadoopIO.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {  //在reduce端连接中，标记气象站数据集的mapper
    private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (parser.parse(value)) {
            context.write(new TextPair(parser.getStationId(), "0"), new Text(parser.getStationName()));  //TextPair的第二个字段标记为0以区分它是气象站记录
        }
    }
}
