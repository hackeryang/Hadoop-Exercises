package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MaxTemperatureSpark {  //找出每个年份的最高气温
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);
        JavaRDD<String> lines = sc.textFile(args[0]);  //RDD实例，即一个输入文件

        JavaRDD<String[]> records = lines.map(new Function<String, String[]>() {  //把文本行拆分为多个字段，将每一行文本（即一个String)拆分为一个String类型的Scala数组
            public String[] call(String s) throws Exception {
                return s.split("\t");
            }
        });

        JavaRDD<String[]> filtered = records.filter(new Function<String[], Boolean>() {  //用过滤器来滤除不良温度记录，例如温度值缺失为9999，状态码并非指定的几个数字等
            public Boolean call(String[] rec) throws Exception {
                return rec[1] != "9999" && rec[2].matches("[01459]");
            }
        });

        JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(new PairFunction<String[], Integer, Integer>() {  //输入RDD为键值对时，使用JavaPairRDD

            public Tuple2<Integer, Integer> call(String[] rec) throws Exception {  //reduceByKey()方法提供分组功能，可以把温度数据按年份字段分组，但该方法需要用Scala Tuple2表示的键值对RDD，因此首先用另一个mapToPair()把字段RDD转换为适当的形式，将年份和温度两个字段形成一个个tuple
                return new Tuple2<Integer, Integer>(Integer.parseInt(rec[0]), Integer.parseInt(rec[1]));
            }
        });
        //将元组(tuple)进行聚合，reduceByKey()方法的参数是一个函数，该函数以一对对键值对作为输入，并将相同的键组合起来形成一个值
        JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return Math.max(i1, i2);
            }
        });
        maxTemps.saveAsTextFile(args[1]);  //saveAsTextFile()方法也会触发Spark作业的运行，和reduceByKey()的主要区别在于它没有任何返回值，只是计算得到一个RDD，并将其分区写入指定目录下的文件中
    }
}
