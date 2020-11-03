package com.zjtd.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户App商城行为数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MarketingUserBehavior {
    public String userId;
    public String channel;
    public String behavior;
    public Long timestamp;

}
