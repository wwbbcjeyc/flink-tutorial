package com.zjtd.other;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;

public class StringToMapActivity {

    public static void main(String[] args) throws UnsupportedEncodingException {

        String url = "/pages/?isappinstalled=0&from=singlemessage#/case/detail/44813?ut_sk=1.WNybZLYisDEDABBnyFqv%2FDQR_23010479_1595033253814.Weixin.(null)&=&from=singlemessage&isappinstalled=0";
        int page_index = String.valueOf(url).indexOf("?");
       // System.out.println(page_index);
        String unCodePage = StringUtils.substring(String.valueOf(url), page_index + 1);
        //System.out.println(unCodePage);
        String ecodePage = URLDecoder.decode(unCodePage, "utf-8");
       System.out.println(ecodePage);
        Map<String,String> result = new HashMap<String,String>();
        String[] params = ecodePage.split("\\&");
        //System.out.println(params.toString());
        for (String entry : params) {
            /*System.out.println("#####################");
            System.out.println(entry);*/
            if (entry.contains("=")) {
                String[] sub = entry.split("\\=");
                //System.out.println(sub);
                if(sub.length>0){
                    if (sub.length>1) {
                       /* System.out.println(sub.length);
                        System.out.println(sub[0]);
                        System.out.println(sub[1]);*/
                        result.put(sub[0], sub[1]);
                    } else {
                        result.put(sub[0], "");
                    }
                }
            }
        }

        /* Set<String> keys = result.keySet();
        for (Object key:keys){
            System.out.println(key+"->"+result.get(key));
        }*/

        /* Collection<String> values = result.values();
         Iterator<String> iter = values.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }*/

        final Set<Map.Entry<String, String>> entries = result.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            System.out.println("key:"+entry.getKey()+", value:"+entry.getValue());
        }

        /*if(page_index>0){
            String unCodePage = StringUtils.substring(String.valueOf(url), page_index + 1);
            String ecodePage = URLDecoder.decode(unCodePage, "utf-8");
            Map<String,String> result = new HashMap<String,String>();
            String[] params = ecodePage.split("\\&");
            for (String entry : params) {
                if (entry.contains("=")) {
                    String[] sub = entry.split("\\=");
                    if (sub.length > 0 && sub.length>1) {
                        result.put(sub[0], sub[1]);
                    } else {
                        result.put(sub[0], "");
                    }
                }
            }
            //output.put("utm",JSON.toJSONString(result));
            System.out.println(JSON.toJSONString(result));
        }else {
            System.out.println(JSON.toJSONString(""));
            //output.put("utm","");
        }*/


        //String url1 ="/pages/?isappinstalled=0&from=singlemessage#/case/detail/44813?ut_sk=1.WNybZLYisDEDABBnyFqv%2FDQR_23010479_1595033253814.Weixin.(null)&=&from=singlemessage&isappinstalled=0";
      /*  System.out.println(url1.lastIndexOf("?"));
        String aa = url1.substring(url1.lastIndexOf("?")+1, url.length());*/

       /* int page_index = url1.lastIndexOf("?");*/
       /* System.out.println(page_index);*/
       /* String aa = url1.substring(url1.lastIndexOf("?")+1, url1.length());*/
       /* System.out.println(aa);*/
       /*  String ecodePage = URLDecoder.decode(aa, "utf-8");*/
       /* System.out.println(ecodePage);*/


       /* if(page_index>0){*/
       /*     String unCodePage = StringUtils.substring(String.valueOf(defaultVariable.get("page")), page_index + 1);*/
       /*     String ecodePage = URLDecoder.decode(unCodePage, "utf-8");*/
       /*     Map<String,String> result = new HashMap<String,String>();*/
       /*     String[] params = ecodePage.split("\\&");*/
       /*     for (String entry : params) {*/
       /*         if (entry.contains("=")) {*/
       /*             String[] sub = entry.split("\\=");*/
       /*             if (sub.length > 1) {*/
       /*                 result.put(sub[0], sub[1]);*/
       /*             } else {*/
       /*                 result.put(sub[0], "");*/
       /*             }*/
       /*         }*/
       /*     }*/
       /*     output.put("utm", JSON.toJSONString(result));*/
       /* }else {*/
       /*     output.put("utm","");*/
       /* }*/




    }
}
