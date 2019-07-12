package HadoopIO;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {  //定制一个新的存储一对Text对象的Writable实现
    private Text first;
    private Text second;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public void write(DataOutput out) throws IOException {  //将每个Text对象序列化到输出流中，因为继承了WritableComparable所以必须实现该方法
        first.write(out);
        second.write(out);
    }

    public void readFields(DataInput in) throws IOException {  //查看各个字段的值，对来自输入流的字节进行反序列化，因为继承了WritableComparable所以必须实现该方法
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {  //MapReduce中的默认分区类HashPartitioner通常用hashCode()方法选择reduce分区，需要确保有个较好的哈希函数来保证每个reduce分区的大小相似
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {  //即使结合使用TextOutputFormat和定制的Writable，也需要自己重写toString()方法，TextOutputFormat对键和值调用toString()方法
        return first + "\t" + second;
    }

    public int compareTo(TextPair tp) {  //如果第一个字符相同，则按照第二个字符排序，因为继承了WritableComparable所以必须实现该方法
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }

    //用于比较TextPair字节表示的RawComparator
    public static class Comparator extends WritableComparator {  //前面的代码是先通过readFields()将数据流反序列化为对象，再通过compareTo方法比较，这里变为直接比较两个TextPair对象的序列化表示，提高速度
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);  //计算字节流中第一个TextPair对象中第一个Text字段的长度，由ByteWritable字节数组开头表示字符个数的字节长度加上实际字符的个数组成，具体原因可看BytesWritable的说明
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);  //计算字节流中第二个TextPair对象中第一个Text字段的长度，由ByteWritable字节数组开头表示字符个数的字节长度加上实际字符的个数组成，具体原因可看BytesWritable的说明
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);  //比较两个TextPair对象的第一个Text对象
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);  //如果两个TextPair对象的第一个Text对象比较结果相同，则比较两者的第二个Text对象
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {  //调用静态方法define()将Comparator注册到WritableComparator的comparators成员中，comparators是HashMap类型而且是static的，相当于告诉WritableComparator，当使用WritableComparator.get(TextPair.class)方法时，要返回自己注册的这个Comparator，然后就可以用comparator.compare()来进行比较，而不需要将要比较的字节流反序列化为对象，节省创建对象的所有开销
        WritableComparator.define(TextPair.class, new Comparator());
    }

    public static class FirstComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);  //调用父类WritableComparator的构造函数
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  //用于比较第一个字段
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).first.compareTo(((TextPair) b).first);
            }
            return super.compare(a, b);
        }
    }
}
