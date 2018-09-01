import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object MaxTemperature {  //使用Spark找出最高温度的Scala应用程序
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf().setAppName("Max Temperature")  //创建属性配置的新实例，可以把各种Spark属性传递给应用
    val sc=new SparkContext(conf)  //shell环境会自动创建SparkContext，代码中需要自己创建

    sc.textFile(args(0))  //利用命令行参数指定输入路径，利用方法链避免为每个RDD创建中间变量
      .map(_.split("\t"))  //“_”符号表示匹配所有的值或对象，与Java的“*”通配符类似。这里把文本行拆分为多个字段，将每一行文本（即一个String)拆分为一个String类型的Scala数组
      .filter(rec => (rec(1) != "9999" && rec(2).matches("[01459]")))  //用过滤器来滤除不良温度记录，例如温度值缺失为9999，状态码并非指定的几个数字等
      .map(rec => (rec(0).toInt,rec(1).toInt))  //reduceByKey()方法提供分组功能，可以把温度数据按年份字段分组，但该方法需要用Scala Tuple表示的键值对RDD，因此首先用另一个map()把RDD转换为适当的形式，将年份和温度两个字段形成一个个tuple
      .reduceByKey((a,b) => Math.max(a,b))  //将元组(tuple)进行聚合，reduceByKey()方法的参数是一个函数，该函数以一对对键值对作为输入，并将相同的键组合起来形成一个值
      .saveAsTextFile(args(1))  //saveAsTextFile()方法也会触发Spark作业的运行，和reduceByKey()的主要区别在于它没有任何返回值，只是计算得到一个RDD，并将其分区写入指定目录下的文件中
  }
}
