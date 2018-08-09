package MapReduceApplication;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {  //解析NCDC格式的气温纪录，与MaxTemperatureMapper配合的解析类
    private static final int MISSING_TEMPERATURE=9999;

    private String year;
    private int airTemperature;
    private String quality;
    private boolean airTemperatureMalformed;

    public void parse(String record){
        year=record.substring(15,19);
        airTemperatureMalformed=false;
        //Remove leading plus sign as parseInt doens't like them(pre-Java 7)
        if(record.charAt(87)=='+'){
            airTemperature=Integer.parseInt(record.substring(88,92));
        }else if(record.charAt(87)=='-'){
            airTemperature=Integer.parseInt(record.substring(87,92));
        }else{
            airTemperatureMalformed=true;
        }
        quality=record.substring(92,93);
    }
    public void  parse(Text record){
        parse(record.toString());
    }
    public boolean isValidTemperature(){  //通过温度数值是否丢失以及质量码是否匹配来确定温度数据是否有效
        return !airTemperatureMalformed && airTemperature!=MISSING_TEMPERATURE && quality.matches("[01459]");
    }
    public boolean isMalformedTemperature(){
        return airTemperatureMalformed;
    }
    public String getYear(){
        return year;
    }
    public int getAirTemperature(){
        return airTemperature;
    }
}
