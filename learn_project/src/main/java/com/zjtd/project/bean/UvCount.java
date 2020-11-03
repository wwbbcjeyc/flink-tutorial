package com.zjtd.project.bean;

import lombok.Data;

/** uv 输出类型 **/
@Data
public class UvCount {
    public Long windowEnd;
    public Long count;

    public static UvCount of(long windowEnd, long count) {
        UvCount result = new UvCount();
        result.windowEnd = windowEnd;
        result.count = count;
        return result;
    }

}
