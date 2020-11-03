package com.zjtd.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** 用户行为数据结构 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {

    public long userId; //用户ID
    public long itemId; //商品ID
    public int categoryId; //商品类目ID
    public String behavior; //用户行为,包括{"pv","buy","cart","fav"}
    public long timestamp; //行为发生的时间戳,单位秒


}
