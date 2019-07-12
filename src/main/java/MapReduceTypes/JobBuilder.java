package MapReduceTypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class JobBuilder {  //MinimalMapReduceWithDefaults类的依赖类,把打印使用说明的逻辑抽取出来并把输入输出路径设定放到这样一个帮助方法中，实现对run()方法的前几行进行简化
    public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {
        if (args.length != 2) {
            printUsage(tool, "<input> <output>");
            return null;
        }

        Job job = new Job(conf);  //Job对象指定作业执行规范，用于控制整个作业的运行，并设置作业的名称,不设置的默认情况下作业名称是JAR文件名
        job.setJarByClass(tool.getClass());  //不必明确指定jar文件的名称，在setJarByClass()方法中传递一个类即可，Hadoop利用这个类查找包含它的jar文件
        FileInputFormat.addInputPath(job, new Path(args[0]));  //定义输入数据的路径，多路径输入可以多次调用该方法
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  //指定输出路径，指定的是reduce()函数输出文件的写入目录运行作业前输出目录不应该存在，否则会报错并拒绝运行作业
        return job;
    }

    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);  //打印命令行应使用的参数信息
    }
}
