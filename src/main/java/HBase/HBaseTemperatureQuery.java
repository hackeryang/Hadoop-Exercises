package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class HBaseTemperatureQuery extends Configured implements Tool {  //检索HBase表中某范围内气象站观测数据行
    static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");  //设置列族
    static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");  //设置列族冒号后面的后缀具体列，即data:airtemp

    public NavigableMap<Long, Integer> getStationObservations(Table table, String stationId, long maxStamp, int maxCount) throws IOException {
        byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);  //获得第一行数据的行键，即观测站ID与逆序时间戳的组合键
        NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();  //创建一个已排序的并可以进行导航（如获取大于等于某对象的键值对）的键值对对象
        Scan scan = new Scan(startRow);  //定义扫描数据的起始行键
        scan.addColumn(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);  //限制扫描HBase表所返回的列
        ResultScanner scanner = table.getScanner(scan);  //遍历获取整个observations表的数据，相当于hbase shell中的scan命令
        try {
            Result res;
            int count = 0;
            while ((res = scanner.next()) != null && count++ < maxCount) {  //当还能扫描到下一行数据并且还没有超过10行
                byte[] row = res.getRow();  //找到当前行键
                byte[] value = res.getValue(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);  //获取当前行的data:airtemp列的单元格的值
                Long stamp = Long.MAX_VALUE - Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);  //将逆序时间戳还原为正常时间戳，即MAX_VALUE-(MAX_VALUE-timestamp)
                Integer temp = Bytes.toInt(value);
                resultMap.put(stamp, temp);  //将正常时间戳和温度值放入resultMap
            }
        } finally {
            scanner.close();
        }
        return resultMap;
    }

    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureQuery <station_id>");
            return -1;
        }

        Configuration config = HBaseConfiguration.create();  //获取HBase的配置文件实例
        Connection connection = ConnectionFactory.createConnection(config);  //根据配置文件创建Connection实例，可以从Connection实例中获取旧API中的HBaseAdmin和HTable对象的功能
        try {
            TableName tableName = TableName.valueOf("observations");  //设置表名
            Table table = connection.getTable(tableName);  //获取observations表的实例，通过Table对象和Connection.getTable()方法代替旧API中的HTable对象
            try {
                NavigableMap<Long, Integer> observations = getStationObservations(table, args[0], Long.MAX_VALUE, 10).descendingMap();  //请求最近10个观测值，并调用descendingMap()使返回值以降序排列
                for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
                    //打印日期，时间以及温度
                    System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(), observation.getValue());
                }
                return 0;
            } finally {
                table.close();
            }
        } finally {
            connection.close();
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseTemperatureQuery(), args);
        System.exit(exitCode);
    }
}
