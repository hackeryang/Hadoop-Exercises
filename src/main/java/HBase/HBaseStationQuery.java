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
import java.util.LinkedHashMap;
import java.util.Map;

public class HBaseStationQuery extends Configured implements Tool {  //从stations表中根据行键stationId来查询一行数据
    static final byte[] INFO_COLUMNFAMILY = Bytes.toBytes("info");  //设置列族info
    static final byte[] NAME_QUALIFIER = Bytes.toBytes("name");  //设置列族冒号后的修饰符，即具体列info:name
    static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");  //设置列族冒号后的修饰符，即具体列info:location
    static final byte[] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");  //设置列族冒号后的修饰符，即具体列info:description

    public Map<String, String> getStationInfo(Table table, String stationId) throws IOException {
        Get get = new Get(Bytes.toBytes(stationId));  //获取行键为stationId的一行数据
        get.addFamily(INFO_COLUMNFAMILY);  //只获得指定列族的数据，即info列族
        Result res = table.get(get);  //根据HBase表对象以及Get对象的设置获得数据
        if (res == null) {
            return null;
        }
        Map<String, String> resultMap = new LinkedHashMap<String, String>();  //将获取的查询结果Result对象转换为更便于使用的由String类型键值对构成的Map
        resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
        resultMap.put("location", getValue(res, INFO_COLUMNFAMILY, LOCATION_QUALIFIER));
        resultMap.put("description", getValue(res, INFO_COLUMNFAMILY, DESCRIPTION_QUALIFIER));
        return resultMap;
    }

    private static String getValue(Result res, byte[] cf, byte[] qualifier) {  //从Result对象中获取具体列内单元格的值
        byte[] value = res.getValue(cf, qualifier);
        return value == null ? "" : Bytes.toString(value);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: HBaseStationQuery <Station_id>");
            return -1;
        }

        Configuration config = HBaseConfiguration.create();  //获取HBase的配置文件实例
        Connection connection = ConnectionFactory.createConnection(config);  //根据配置文件创建Connection实例，可以从Connection实例中获取旧API中的HBaseAdmin和HTable对象的功能
        try {
            TableName tableName = TableName.valueOf("stations");  //设置表名
            Table table = connection.getTable(tableName);  //获取stations表的实例，通过Table对象和Connection.getTable()方法代替旧API中的HTable对象
            try {
                Map<String, String> stationInfo = getStationInfo(table, args[0]);  //根据命令行中输入的观测站ID来从HBase表中生成Map类型的数据
                if (stationInfo == null) {
                    System.err.printf("Station ID %s not found.\n", args[0]);
                    return -1;
                }
                for (Map.Entry<String, String> station : stationInfo.entrySet()) {
                    System.out.printf("%s\t%s\n", station.getKey(), station.getValue());  //遍历打印出对应观测站ID的三条resultMap中的键值对
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
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseStationQuery(), args);
        System.exit(exitCode);
    }
}
