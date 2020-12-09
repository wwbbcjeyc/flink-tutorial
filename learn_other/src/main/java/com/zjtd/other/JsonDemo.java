package com.zjtd.other;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.orc.OrcSplitReader;

public class JsonDemo {

    public static void main(String[] args) {
        String input = "{\"serverTime\":1596269424778,\"defaultVariable\":{\"ip\":\"114.242.236.137\"}}";
        JSONObject jsonObject = JSONObject.parseObject(input);
       /* System.out.println(jsonObject.toJSONString());
        System.out.println(jsonObject.getLong("server")==null);
        System.out.println(jsonObject.getJSONObject("eventVariable")==null);
        System.out.println(jsonObject.getString("token") =="");
        System.out.println(StringUtils.isEmpty(jsonObject.getString("token")));*/


        String jsonStr = "{\r\n" + "\"name\":\"jarWorker\",\r\n" + "\"sex\":\"\",\r\n" + "\"age\":26,\r\n"
                + "\"love\":[{\"hobby\":\"足球\",\"color\":\"White\"},{\"hobby\":\"篮球\",\"color\":\"Brown\"},{\"hobby\":\"简书\",\"color\":\"Yellow\"}],\r\n"
                + "\"goodAt\":\"Java\"\r\n" + "}";
        JSONObject js = JSON.parseObject(jsonStr);
        //System.out.println(js.toJSONString());
        String value = String.valueOf(js.get("name1"));
        String value1 = js.getString("name2");
        /*System.out.println("value:"+value);
        System.out.println("value1:"+value1);*/

        Long age1 = js.getLong("age1");
        Long age2 = Long.valueOf(String.valueOf(js.get("age")));

        /*System.out.println(StringUtils.isNotEmpty(value1));
        System.out.println(age1==null);
        System.out.println(js.get("age2")==null);
        System.out.println(age2);

        String hobby = String.valueOf(js.get("sex"));
        System.out.println("hobby:"+hobby);*/

       /* for (int i = 0; i < value1.getBytes().length; i++) {
            System.out.println(i);
        }*/

        String str1 = "{\"eventSn\":\"longArticlePageView\",\"sendingTime\":1600683367767,\"serverTime\":1600683363808,\"token\":\"dc9ccabf30b2e41e36b6fb855f2f95f4\",\"eventVariable\":{\"resourceId\":\"5218697\"},\"defaultVariable\":{\"report_method\":\"101\",\"app_version\":\"3.5.0\",\"os\":\"Android\",\"open_id\":\"oQYbCwH_HDD6AgwEVSTfbDKBJTZg\",\"os_version\":\"10\",\"triggerTime\":\"1600683367766\",\"title\":\"文章详情\",\"uuid\":\"074435dbb42a429d9f5af25f87a96ffe\",\"platform\":\"android\",\"manufacturer\":\"OnePlus\",\"partnerNo\":\"xiaobang\",\"domain\":\"com.xiaobang.fq\",\"sdk_version\":\"1.3.0\",\"model\":\"IN2020\",\"page\":\"/XbMainActivity/ArticleResourceActivity\",\"network_type\":\"wifi\",\"udid\":\"e54cb8a1-aad6-3fd7-9487-bac70741d0ea\",\"ip\":\"114.242.236.137\"}}\n";
        String str2="";
        JSONObject str1Obj = JSON.parseObject(str1);
        System.out.println(str1Obj==null);
        System.out.println(str1Obj==null);
        System.out.println(StringUtils.isNotBlank(str1Obj.getString("name") )|| null==str1Obj.getString("name"));

        System.out.println(str1Obj.getString("EVE"));

    }
}
