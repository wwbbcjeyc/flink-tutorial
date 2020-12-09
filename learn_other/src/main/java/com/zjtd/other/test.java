package com.zjtd.other;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author wangwenbo
 * @Date 2020/11/9 5:47 下午
 * @Version 1.0
 */
public class test {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        //String str = ",,,,,,,,aa,,,,,,,,a,,aaa,,,,aaas,,,,,,,,,,,,,,,,";
        //str = str.replaceAll("[',']+", ",");
        //System.out.println(str);
        //select regexp_replace(filed,'[',']+',',')

        String date = "'?????????L'ALPINA???????'";
        //String time= date.replaceAll("\\'","");
        //System.out.println(time);//结果：广告源请求事件


        String orgStr ="'' '''你''好''''张 三_李素：王？五：《赵六》''";
        String regEx="[\\s~·`!！@#￥$%^……&*（()）\\-——\\-_=+【\\[\\]】｛{}｝\\|、\\\\；;：:‘'“”\"，,《<。.》>、/？?]";
        //String regExNew="[\\s~`'']";
        //方法一：
//        Pattern p = Pattern.compile(regEx);
//        Matcher m = p.matcher(orgStr);
//        System.out.println(m.replaceAll(""));

        // 方法二：
       // String s = replaceAll(regEx, "");
       // System.out.println(s);


        ArrayList<String> strList = new ArrayList<>();

        strList.add("1'''''''");
        strList.add("2'''3''''");
        strList.add("3'''''''");
        strList.add("4'''''''");
        strList.add("5'''''''");
        //System.out.println(converToCsv(strList));

        String value = "('872eb0dd2508478b945fafcbcd6aaabe', '173fae097e457510_c7b1d56a', '173fae097e457510_c7b1d56a', 'MIDAS_UNIT_REQUEST', '广告位请求事件', 'ad_process', '2020-12-02 09:53:31.069', '2020-12-02 09:53:32', '458', '1', '3.11.1_dev_20201201', '1.3.0.2', 'jwt_test', '', '', '', 'adpos_9081378161', '', '', '1', '', '', '17108', '3', '', '200', '200', '', '131' )";
        value.split(",");
        System.out.println("".split(","));

    }

    private static String converToCsv(List<String> list) {
       // String regEx="[\\s~`'']";
        String regEx="[\\s~·`!！@#￥$%^……&*（()）\\-——\\-_=+【\\[\\]】｛{}｝\\|、\\\\；;：:‘'“”\"，,《<。.》>、/？?]";
        String joinStr = list.stream().map(s -> "\'" + s.replaceAll(regEx, "") + "\'").collect(Collectors.joining(","));
        return String.format("%s%s%s","(",joinStr,")");


    }



}
