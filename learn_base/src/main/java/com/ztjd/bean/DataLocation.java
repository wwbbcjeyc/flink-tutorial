package com.ztjd.bean;

/**
 * TODO 实体类
 */
public class DataLocation {

    public String id;

    public String name;

    public String date;

    public String province;

    public DataLocation() {
    }

    public DataLocation(String id, String name, String date, String province) {
        this.id = id;
        this.name = name;
        this.date = date;
        this.province = province;
    }

    public static DataLocation of(String id, String name, String date, String province) {
        return new DataLocation(id, name, date, province);
    }

    @Override
    public String toString() {
        return "DataLocation{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", date='" + date + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
