package HBase;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyConverter {  //HBaseTemperatureImporter的依赖类，用于将观测站ID和观测时间结合创建HBase表的行键
    private static final int STATION_ID_LENGTH = 12;  //设置观测站ID的固定长度，使行键能够正确排序

    /*
     * @return a row key whose format is: <station_id> <reverse_order_timestamp>
     * */
    public static byte[] makeObservationRowKey(String stationId, long observationTime) {
        byte[] row = new byte[STATION_ID_LENGTH + Bytes.SIZEOF_LONG];  //Bytes.SIZEOF_LONG参量用于计算行键字节数组的时间戳部分的长度，与观测站ID长度相加组成行键长度
        Bytes.putBytes(row, 0, Bytes.toBytes(stationId), 0, STATION_ID_LENGTH);  //在字节数组row的相对偏移量位置，即数组开头填充观测站ID
        long reverseOrderTimestamp = Long.MAX_VALUE - observationTime;  //将Long数字的最大值减去实际时间戳得到逆序时间戳，便于最新的观测数据排在最前面显示
        Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderTimestamp);  //在字节数组row的相对偏移量位置，即观测站ID长度后的位置填充逆序时间戳
        return row;
    }
}
