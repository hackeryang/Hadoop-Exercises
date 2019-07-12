package MapReduceApplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MaxTemperatureDriverTest {  //测试作业驱动程序的输出是否满足预期
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();  //根据编辑好的xml配置文件创建Configuration实例
        conf.set("fs.defaultFS", "file:///");  //设置文件系统为本地文件系统
        conf.set("mapreduce.framework.name", "local");  //设置为使用本地作业运行器
        conf.setInt("mapreduce.task.io.sort.mb", 1);  //设置mapreduce中间输出使用环形缓冲区缓存在本地内存的大小，默认为100M

        Path input = new Path("/mnt/sda6/NCDC.txt");
        Path output = new Path("/mnt/sda6/output");

        FileSystem fs = FileSystem.getLocal(conf);  //通过给定的配置权限确定要使用的本地文件系统
        fs.delete(output, true);  //删除上一次运行后的旧输出文件夹以免报错

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]{
                input.toString(), output.toString()
        });
        assertThat(exitCode, is(0));

        //checkOutput(conf,output);  //逐行对比实际输出与预期输出
    }
}
