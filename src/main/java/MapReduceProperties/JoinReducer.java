package MapReduceProperties;

import HadoopIO.TextPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair,Text,Text, Text> {  //用于连接已标记的气象站数据集和天气记录数据集的reducer

    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter=values.iterator();  //迭代部分中，为了提高效率对象被重复使用，因此下一行的语句很关键，如果没有该语句，stationName就会指向上一条记录的值
        Text stationName=new Text(iter.next());
        while(iter.hasNext()){
            Text record=iter.next();
            Text outValue=new Text(stationName.toString()+"\t"+record.toString());
            context.write(key.getFirst(),outValue);  //输出的键为气象站ID，值为气象站名称和天气记录
        }
    }
}
