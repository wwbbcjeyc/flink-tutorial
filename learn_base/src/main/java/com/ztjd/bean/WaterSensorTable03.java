package com.ztjd.bean;

public class WaterSensorTable03 {
    //传感器ID
    private String id;
    //时间戳
    private String ts;
    //空高，这里为了演示方便，认为是 水位高度
    private String vc;

    public WaterSensorTable03() {
    }

    public WaterSensorTable03(String id, String ts, String vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getVc() {
        return vc;
    }

    public void setVc(String vc) {
        this.vc = vc;
    }

    @Override
    public String toString() {
        return "WaterSensorTable03{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}
