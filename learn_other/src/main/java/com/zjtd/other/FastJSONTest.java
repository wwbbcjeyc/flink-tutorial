package com.zjtd.other;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class FastJSONTest {
    public static void main(String[] args){

        JSONObject obj = new JSONObject();
       // System.out.println(obj.isEmpty()); //true
        obj.put("name", "luolei");
       // System.out.println(obj.isEmpty()); //false

        Map<String,Object> map = new HashMap<>();
        map.put("age", 24);
        map.put("name", "");
        String jsonString = JSON.toJSONString(map);
        System.out.println("json字符串是：" + jsonString);

    }

}
