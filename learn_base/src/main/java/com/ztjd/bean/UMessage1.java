package com.ztjd.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UMessage1 {

    private String uid;
    private String evenSn;
    private String flowId;
    private String createTime;

    public UMessage1() {
    }

    public UMessage1(String uid,String evenSn,String flowId,String createTime) {
        this.uid = uid;
        this.evenSn = evenSn;
        this.flowId = flowId;
        this.createTime = createTime;
    }

}
