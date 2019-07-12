package MapReduceApplication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {  //使用Apache Commons Logging API将任务日志写到标准输出
    private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

    @Override
    @SuppressWarnings("unchecked")  //该批注允许选择性地取消特定代码段中的警告，这里抑制了未检查操作的警告，例如使用List，ArrayList等未进行参数化产生的警告
    protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
        //Log to stdout file
        System.out.println("Map key: " + key);
        //Log to sysLog file
        if (LOG.isDebugEnabled()) {
            LOG.debug("Map value: " + value);
        }
        context.write((KEYOUT) key, (VALUEOUT) value);
    }
}
