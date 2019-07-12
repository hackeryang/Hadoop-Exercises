package MapReduceProperties;

import MapReduceApplication.NcdcRecordParser;
import MapReduceTypes.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxTemperatureUsingSecondarySort extends Configured implements Tool {  //对键和值组成的组合键中的气温进行排序来找出最高气温

    //自己定义的key类应该实现WritableComparable接口
    public static class IntPair implements WritableComparable<IntPair> {
        @Override
        public String toString() {
            return first + "\t" + second;
        }

        private int first;
        private int second;

        public IntPair() {  //无参构造函数一定要显示声明，因为框架会通过反射在内部生成该类对象，在KeyComparator中会用到

        }

        //设置组合键的前后两个值
        public IntPair(int first, int second) {
            this.first = first;
            this.second = second;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        //反序列化，从流中的二进制转换成IntPair，因为继承了WritableComparable所以必须实现该方法
        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }

        //序列化，将IntPair转化成使用流传输的二进制，因为继承了WritableComparable所以必须实现该方法
        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        //重载compareTo方法，进行组合键key的比较，分组后的二次排序会隐式调用该方法，因为继承了WritableComparable所以必须实现该方法
        public int compareTo(IntPair o) {
            if (o instanceof IntPair) {
                int cmp = (first == o.first) ? 0 : ((first > o.first) ? -1 : 1);  //因为是倒序排列，最大的排在最前面，所以当大于时输出-1，与默认情况相反
                if (cmp != 0) return cmp;
            }
            return (second == o.second) ? 0 : ((second > o.second) ? -1 : 1);
        }

    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()) {
                context.write(new IntPair(parser.getYearInt(), parser.getAirTemperature()), NullWritable.get());  //NullWritable.get()用于获取空值，不能像其他Writable一样用new NullWritable来获取
            }
        }
    }

    static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {  //创建自定义partitioner以按照组合键的首字段即年份进行分区，保证同一年的记录会被发送到同一个reducer中
        @Override
        public int getPartition(IntPair key, NullWritable value, int numPartitions) {
            //multiply by 127 to perform some mixing
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {  //对组合键进行降序排序
        protected KeyComparator() {
            super(IntPair.class, true);  //调用父类WritableComparator的构造函数
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return ip1.compareTo(ip2);
        }
    }

    public static class GroupComparator extends WritableComparator {  //按年份对键进行分组，只取键的首字段进行比较，按照年份升序排序
        protected GroupComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            return Integer.compare(ip1.getFirst(), ip2.getFirst());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);  //把打印使用说明的逻辑抽取出来并把输入输出路径设定放到这样一个帮助方法中，实现对run()方法的前几行进行简化
        if (job == null) {
            return -1;
        }

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setPartitionerClass(FirstPartitioner.class);  //设置用于分区的类
        job.setSortComparatorClass(KeyComparator.class);  //设置用于排序的comparator类
        job.setGroupingComparatorClass(GroupComparator.class);  //设置用于分组的comparator类
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);  //exitCode被赋予上面run()方法中最后job.waitForCompletion()的返回值
        System.exit(exitCode);
    }
}
