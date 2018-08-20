package MapReduceProperties;

import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NcdcStationMetadata {  //MaxTemperatureByStationNameUsingDistributedCacheFile类下mapper类需要用到的依赖类，用于解析气象站ID和气象站名称

    private Map<String,String> stationIdToName=new HashMap<String,String>();

    public void initialize(File file) throws IOException {
        BufferedReader in=null;
        try{
            in=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            NcdcStationMetadataParser parser=new NcdcStationMetadataParser();
            String line;
            while((line=in.readLine())!=null){
                if(parser.parse(line)){
                    stationIdToName.put(parser.getStationId(),parser.getStationName());  //将从文件输入读到并解析出来的气象站ID与气象站名称以键值对形式存到HashMap中
                }
            }
        }finally{
            IOUtils.closeStream(in);
        }
    }

    public String getStationName(String stationId){
        String stationName=stationIdToName.get(stationId);  //根据HashMap中的键，即气象站ID查找值，即气象站名称
        if(stationName==null || stationName.trim().length()==0){
            return stationId;  //没找到对应的气象站名称就只返回气象站ID
        }
        return stationName;
    }

    public Map<String,String> getStationIdToNameMap(){
        return Collections.unmodifiableMap(stationIdToName);  //返回HashMap内包含的气象站ID与名称键值对的全部集合，但是返回的集合不能再被更改
    }
}
