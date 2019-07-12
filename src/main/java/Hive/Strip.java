package Hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Strip extends UDF {  //剪除字符串尾字符的UDF
    private Text result = new Text();

    public Text evaluate(Text str) {  //必须重写evaluate()方法，去除输入首尾两端的空白字符
        if (str == null) {
            return null;
        }

        result.set(StringUtils.strip(str.toString()));
        return result;
    }

    public Text evaluate(Text str, String stripChars) {  //去除字符串首尾两端中包含在指定字符集合(stripChars)里的任何字符
        if (str == null) {
            return null;
        }
        result.set(StringUtils.strip(str.toString(), stripChars));
        return result;
    }
}
