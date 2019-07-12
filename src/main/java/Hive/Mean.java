package Hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class Mean extends UDAF {  //计算一组double值平均值的UDAF，需要继承UDAF类

    //需要包含一至多个嵌套的实现了UDAFEvaluator接口的静态类，用于提供double平均值计算的UDAF重载
    public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator {
        public static class PartialResult {
            double sum;
            long count;
        }

        private PartialResult partial;

        public void init() {  //必须实现该方法，负责初始化计算函数并设置它的内部状态
            partial = null;
        }

        //必须实现该方法，每次对一个新值进行聚集计算时都会调用该方法，括号中接受的参数和Hive中被调用函数的参数是对应的
        public boolean iterate(DoubleWritable value) {  //Hive中的DoubleWritable类可以自动序列化和反序列化
            if (value == null) {  //检查传入参数值是否为空，如果是就忽略
                return true;
            }
            if (partial == null) {
                partial = new PartialResult();  //将自己的结果初始化为新的结果对象
            }
            partial.sum += value.get();  //将输入累加起来形成总和
            partial.count++;  //每读到一个输入，数字个数就加1
            return true;
        }

        public PartialResult terminatePartial() {  //必须实现该方法，Hive需要部分聚集计算结果时会调用，返回目前为止已经计算的当前状态和结果
            return partial;
        }

        //必须实现该方法，在Hive决定要合并一个部分聚集值和另一个部分聚集值时调用，括号内输入对象必须和terminatePartial()返回类型一致
        public boolean merge(PartialResult other) {
            if (other == null) {  //如果没有来自其他分区的部分结果，就不合并
                return true;
            }
            if (partial == null) {  //如果自己的部分结果不存在，则将部分结果初始化为新的结果对象
                partial = new PartialResult();
            }
            partial.sum += other.sum;  //从其他分区的累加结果也加过来合并
            partial.count += other.count;  //其他分区积累的数字个数也累加过来
            return true;
        }

        public DoubleWritable terminate() {  //必须实现该方法，Hive需要最终聚集结果时会调用
            if (partial == null) {
                return null;
            }
            return new DoubleWritable(partial.sum / partial.count);  //返回总的平均值
        }
    }
}
