package com.zjtd.other;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author wangwenbo
 * @version 1.0
 * @date 2020/10/22 5:58 下午
 */
public class TestDemo {
    public static void main(String[] args) {
        Field f1 = new Field("tony", 19);
        Field f2 = new Field("jack", 16);
        Field f3 = new Field("tom", 80);
        Field f4 = new Field("jbson", 44);
        Field f5 = new Field("jason", 44);

        List<Field> list = new ArrayList<Field>();
        list.add(f1);
        list.add(f3);
        list.add(f4);
        list.add(f2);
        list.add(f5);
        Collections.sort(list);

        for (Field o : list) {
            System.out.println(o.getAge() + "-->" + o.getName());
        }
    }
}
