package com.zjtd.other;

/**
 * @Author wangwenbo
 * @Date 2020/11/9 5:47 下午
 * @Version 1.0
 */
public class test {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String str = ",,,,,,,,aa,,,,,,,,a,,aaa,,,,aaas,,,,,,,,,,,,,,,,";
        str = str.replaceAll("[',']+", ",");
        System.out.println(str);
        //select regexp_replace(filed,'[',']+',',')
    }

}
