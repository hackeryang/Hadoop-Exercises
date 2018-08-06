package HadoopIO;

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

public class TextIterator {  //遍历Text对象中的字符
    public static void main(String[] args){
        Text t=new Text("\u0041\u00DF\u6771\uD801\uDC00");

        ByteBuffer buf=ByteBuffer.wrap(t.getBytes(),0,t.getLength());  //将Text字符串放入缓冲区
        int cp;
        while(buf.hasRemaining() && (cp=Text.bytesToCodePoint(buf))!=-1){  //当缓冲区有数据且未遍历到最后一个字符时，通过bytesToCodePoint()方法获取下一个字符的位置，返回相应int值
            System.out.println(Integer.toHexString(cp));
        }
    }
}
