package com.zjtd.project.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** url点击(窗口操作的输出类型) */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageViewCount {
    public String url;
    public Long windowEnd;
    public Long count;

}

