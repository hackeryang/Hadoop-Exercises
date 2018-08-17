package MapReduceTypes;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinimalMapReduce extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if(args.length!=2){
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);  //打印命令行应使用的参数信息
            return -1;
        }

        Job job=new Job(getConf());  //Job对象指定作业执行规范，用于控制整个作业的运行，并设置作业的名称,不设置的默认情况下作业名称是JAR文件名
        job.setJarByClass(getClass());  //不必明确指定jar文件的名称，在setJarByClass()方法中传递一个类即可，Hadoop利用这个类查找包含它的jar文件
        FileInputFormat.addInputPath(job,new Path(args[0]));  //定义输入数据的路径，多路径输入可以多次调用该方法
        FileOutputFormat.setOutputPath(job,new Path(args[1]));  //指定输出路径，指定的是reduce()函数输出文件的写入目录运行作业前输出目录不应该存在，否则会报错并拒绝运行作业
        return job.waitForCompletion(true)?0:1;  //提交作业并等待执行完成，具有一个唯一的标识用于指示是否已生成详细输出，标识为true时作业把其进度信息写到控制台，将true或false转换成程序退出代码0或1
    }

    public static void main(String[] args) throws Exception{
        int exitCode=ToolRunner.run(new MinimalMapReduce(),args);
        System.exit(exitCode);
    }
}
