package MapReduceApplication;

import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NcdcRecordParser {  //解析NCDC格式的气温纪录，与MaxTemperatureMapper配合的解析类
    private static final int MISSING_TEMPERATURE = 9999;

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");

    private String stationId;
    private String observationDateString;
    private String year;
    private String airTemperatureString;
    private int airTemperature;
    private String quality;
    private boolean airTemperatureMalformed;

    public void parse(String record) {
        stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
        observationDateString = record.substring(15, 27);
        year = record.substring(15, 19);
        airTemperatureMalformed = false;
        //Remove leading plus sign as parseInt doens't like them(pre-Java 7)
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else if (record.charAt(87) == '-') {
            airTemperatureString = record.substring(87, 92);
        } else {
            airTemperatureMalformed = true;
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {  //通过温度数值是否丢失以及质量码是否匹配来确定温度数据是否有效
        return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public boolean isMalformedTemperature() {
        return airTemperatureMalformed;
    }

    public String getStationId() {
        return stationId;
    }

    public Date getObservationDate() {
        try {
            System.out.println(observationDateString);
            return DATE_FORMAT.parse(observationDateString);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getYear() {
        return year;
    }

    public int getYearInt() {
        return Integer.parseInt(year);
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getAirTemperatureString() {
        return airTemperatureString;
    }

    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public String getQuality() {
        return quality;
    }
}
