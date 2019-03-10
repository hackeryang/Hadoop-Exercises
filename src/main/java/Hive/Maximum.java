package Hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

public class Maximum extends UDAF {  //计算一组整数最大值的UDAF，需要继承UDAF类

    //需要包含一至多个嵌套的实现了UDAFEvaluator接口的静态类，用于提供整数最大值计算的UDAF重载
    public static class MaximumIntUDAFEvaluator implements UDAFEvaluator{
        private IntWritable result;

        public void init() {  //必须实现该方法，负责初始化计算函数并设置它的内部状态
            result=null;
        }

        //必须实现该方法，每次对一个新值进行聚集计算时都会调用该方法，括号中接受的参数和Hive中被调用函数的参数是对应的
        public boolean iterate(IntWritable value){
            if(value==null){  //检查传入参数值是否为空，如果是就忽略
                return true;
            }
            if(result==null){
                result=new IntWritable(value.get());  //result变量初始化为第一个输入的值
            }else{  //result已赋过值，则与当前输入值比较并覆盖为更大值
                result.set(Math.max(result.get(),value.get()));
            }
            return true;
        }

        public IntWritable terminatePartial(){  //必须实现该方法，Hive需要部分聚集计算结果时会调用，返回目前为止已经计算的当前状态和结果
            return result;
        }

        //必须实现该方法，在Hive决定要合并一个部分聚集值和另一个部分聚集值时调用，括号内输入对象必须和terminatePartial()返回类型一致
        public boolean merge(IntWritable other){
            return iterate(other);
        }

        public IntWritable terminate(){  //必须实现该方法，Hive需要最终聚集结果时会调用
            return result;
        }
    }
}
