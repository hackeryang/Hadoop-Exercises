package HBase;

import MapReduceProperties.NcdcStationMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Map;

public class HBaseStationImporter extends Configured implements Tool {  //用于将HDFS中的stations-fixed-width.txt数据导入到stations表中

    @Override
    public int run(String[] args) throws Exception {
        if(args.length!=1){
            System.err.println("Usage: HBaseStationImporter <input>");
            return -1;
        }

        Configuration config=HBaseConfiguration.create();  //获取HBase的配置文件实例
        Connection connection= ConnectionFactory.createConnection(config);  //根据配置文件创建Connection实例，可以从Connection实例中获取旧API中的HBaseAdmin和HTable对象的功能
        try{
            TableName tableName= TableName.valueOf("stations");
            Table table=connection.getTable(tableName);  //获取stations表的实例，通过Table对象和Connection.getTable()方法代替旧API中的HTable对象
            try{
                NcdcStationMetadata metadata=new NcdcStationMetadata();
                metadata.initialize(new File(args[0]));
                Map<String,String> stationIdToNameMap=metadata.getStationIdToNameMap();

                for(Map.Entry<String,String> entry:stationIdToNameMap.entrySet()){
                    Put put=new Put(Bytes.toBytes(entry.getKey()));
                    put.add(HBaseStationQuery.INFO_COLUMNFAMILY,HBaseStationQuery.NAME_QUALIFIER,Bytes.toBytes(entry.getValue()));  //向stations表中的info:name列插入气象站名称
                    put.add(HBaseStationQuery.INFO_COLUMNFAMILY,HBaseStationQuery.DESCRIPTION_QUALIFIER,Bytes.toBytes("(unknown)"));
                    put.add(HBaseStationQuery.INFO_COLUMNFAMILY,HBaseStationQuery.LOCATION_QUALIFIER,Bytes.toBytes("(unknown)"));
                    table.put(put);
                }
            }finally{
                table.close();
            }
        }finally{
            connection.close();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int exitCode= ToolRunner.run(HBaseConfiguration.create(),new HBaseStationImporter(),args);
        System.exit(exitCode);
    }
}
