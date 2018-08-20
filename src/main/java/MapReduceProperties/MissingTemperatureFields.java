/*
package MapReduceProperties;

import MapReduceTypes.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class MissingTemperatureFields extends Configured implements Tool {  //统计气温信息缺失记录所占的比例

    public int run(String[] args) throws Exception {
        if(args.length!=1){
            JobBuilder.printUsage(this,"<job ID>");
            return -1;
        }
        String jobID=args[0];
        Cluster cluster=new Cluster(getConf());
        Job job=cluster.getJob(JobID.forName(jobID));
        if(job==null){
            return -1;
        }

        Counters counters=job.getCounters();
        //通过枚举值来获取气温信息缺失的记录数和被处理的记录数
        long missing=counters.findCounter(MaxTemperatureWithCounters.Temperature.MISSING).getValue();
        long total=counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();

        System.out.printf("Records with missing temperature fields: %.2f%%\n",100.0*missing/total);
        return 0;
    }
    public static void main(String[] args) throws Exception{
        int exitCode=ToolRunner.run(new MissingTemperatureFields(),args);
        System.exit(exitCode);
    }
}
*/
