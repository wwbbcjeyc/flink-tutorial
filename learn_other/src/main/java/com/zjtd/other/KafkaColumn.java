package com.zjtd.other;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.Map;
import java.util.Set;

public class KafkaColumn {
    public static String getKadfkaColumn(String jsonData) {
        //解析时增加参数不调整顺序(fastjson)
        JSONObject parse = JSONObject.parseObject(jsonData, Feature.OrderedField);
        Set<Map.Entry<String, Object>> entries = parse.entrySet();
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, Object> entry: entries) {
            System.out.println("key:"+entry.getKey());
            sb.append(entry.getKey());
            sb.append(",");
        }
        System.out.println(sb);
        String kafkaColumn = sb.toString().substring(0, sb.length() - 1);
        return   kafkaColumn;
    }

    public static void main(String[] args) {
        String str="{\"id\":\"156015\",\"user\":\"-\",\"time\":\"2019-06-10 16:31:33\",\"ip\":\"127.0.0.1\"}";
        //System.out.println(getKadfkaColumn(str));

        //json串反转义消除反斜杠
        String str1 = "{\"resourceId\":\"dfead70e4ec5c11e43514000ced0cdcaf\",\"properties\":{\"process_id\":\"process4\",\"name\":\"\",\"documentation\":\"\",\"processformtemplate\":\"\"}}";
        String tmp = StringEscapeUtils.unescapeJavaScript(str1);
        System.out.println("tmp:" + tmp);

    }
}
