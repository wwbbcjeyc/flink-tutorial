package com.zitd.example.bean;

public class Result {

    private String type;
    private int uv;
    //      截止到当前时间的时间
    private String dateTime;

    public String getDateTime(){
        return dateTime;
    }

    public void setDateTime(String dateTime){
        this.dateTime = dateTime;
    }

    public String getType(){
        return type;
    }

    public void setType(String type){
        this.type = type;
    }

    public int getUv(){
        return uv;
    }

    public void setUv(int uv){
        this.uv = uv;
    }

    @Override
    public String toString(){
        return "Result{" +
                ", dateTime='" + dateTime + '\'' +
                "type='" + type + '\'' +
                ", uv=" + uv +
                '}';
    }

}
