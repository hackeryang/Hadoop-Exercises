package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ExampleClient {  //基本的HBase表管理与访问，功能与在hbase shell中创建与管理表相同
    public static void main(String[] args) throws IOException {
        Configuration config= HBaseConfiguration.create();  //获取HBase的配置文件实例
        //创建表
        Connection connection=ConnectionFactory.createConnection(config);  //根据配置文件创建Connection实例，可以从Connection实例中获取旧API中的HBaseAdmin和HTable对象的功能
        try{
            Admin admin=connection.getAdmin();  //Admin对象用于管理HBase集群，添加和丢弃表，代替旧API中的HBaseAdmin对象
            try{
                TableName tableName= TableName.valueOf("test");  //设置表名
                HTableDescriptor htd=new HTableDescriptor(tableName);
                HColumnDescriptor hcd=new HColumnDescriptor("data");  //设置列族
                htd.addFamily(hcd);  //添加列族
                admin.createTable(htd);  //根据描述器的设置创建test表
                HTableDescriptor[] tables=admin.listTables();  //在HBase实例中列出所有的表并放入一个表描述器数组中
                if(tables.length!=1 && Bytes.equals(tableName.getName(),tables[0].getTableName().getName())){  //如果未成功创建表则报错
                    throw new IOException("Failed create of table");
                }
                //插入三条输入、获取一行的值、遍历整个表的数据
                Table table=connection.getTable(tableName);  //Table对象用于访问指定的表，通过Connection实例获得以代替旧API中的HTable
                try{
                    for(int i=1;i<=3;i++){
                        byte[] row=Bytes.toBytes("row"+i);  //rowi作为表中每行数据的行键
                        Put put=new Put(row);  //每行作为一个输入
                        byte[] columnFamily=Bytes.toBytes("data");
                        byte[] qualifier=Bytes.toBytes(String.valueOf(i));
                        byte[] value=Bytes.toBytes("value"+i);
                        put.add(columnFamily,qualifier,value);  //插入三行数据，包括列族和列族内的三列，以及值
                        table.put(put);  //将一条输入插入到表中，相当于hbase shell中的put命令
                    }
                    Get get=new Get(Bytes.toBytes("row1"));  //获取第一行的数据
                    Result result=table.get(get);  //相当于hbase shell中的get命令
                    System.out.println("Get: "+result);
                    Scan scan=new Scan();
                    ResultScanner scanner=table.getScanner(scan);  //遍历获取整个test表的数据，相当于hbase shell中的scan命令
                    try{
                        for(Result scannerResult:scanner){
                            System.out.println("Scan: "+scannerResult);
                        }
                    }finally{
                        scanner.close();  //HBase扫描器使用后需要关闭
                    }
                    //使表“离线”并删除表
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }finally{
                    table.close();
                }
            }finally{
                admin.close();
            }
        }finally{
            connection.close();
        }

    }
}
