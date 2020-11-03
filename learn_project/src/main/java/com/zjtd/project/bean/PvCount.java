package com.zjtd.project.bean;

import lombok.Data;

/** pv 输出类型 **/
@Data
public class PvCount {
    public Long windowEnd;
    public Long count;

    public static PvCount of(long windowEnd, long count) {
        PvCount result = new PvCount();
        result.windowEnd = windowEnd;
        result.count = count;
        return result;
    }

}
