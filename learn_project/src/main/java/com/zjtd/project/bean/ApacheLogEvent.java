package com.zjtd.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** apache日志数据结构**/

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLogEvent {
    public String ip;
    public String userId;
    public Long eventTime;
    public String method;
    public String url;
}
