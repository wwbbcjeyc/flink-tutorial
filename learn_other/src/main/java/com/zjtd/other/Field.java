package com.zjtd.other;

/**
 * @author wangwenbo
 * @version 1.0
 * @date 2020/10/22 5:57 下午
 */
public class Field implements Comparable<Field>{
    private String name;
    private int age;

    public Field() {
    }
    public Field(String name, int age) {
        this.name = name;
        this.age = age;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    @Override
    public int compareTo(Field o) {
        // TODO Auto-generated method stub
        // 先按age排序
        if (this.age > o.getAge()) {
            return (this.age - o.getAge());
        }
        if (this.age < o.getAge()) {
            return (this.age - o.getAge());
        }

        // 按name排序
        if (this.name.compareTo(o.getName()) > 0) {
            return 1;
        }
        if (this.name.compareTo(o.getName()) < 0) {
            return -1;
        }

        return 0;
    }

}
