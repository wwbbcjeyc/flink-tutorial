package com.zitd.example.bean;

/**
 * 用于存储聚合的结果(BigScreem)
 */
public class CategoryPojo {

    //		分类名称
    private String category;
    //		改分类总销售额
    private double totalPrice;
    //      截止到当前时间的时间
    private String dateTime;

    public String getCategory(){
        return category;
    }

    public void setCategory(String category){
        this.category = category;
    }

    public double getTotalPrice(){
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice){
        this.totalPrice = totalPrice;
    }

    public String getDateTime(){
        return dateTime;
    }

    public void setDateTime(String dateTime){
        this.dateTime = dateTime;
    }

    @Override
    public String toString(){
        return "CategoryPojo{" +
                "category='" + category + '\'' +
                ", totalPrice=" + totalPrice +
                ", dateTime=" + dateTime +
                '}';
    }
}
