package com.ztjd.bean;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UMessage {

    private String uid;

    private String createTime;

    public UMessage() {
    }

    public UMessage(String uid, String createTime) {
        this.uid = uid;
        this.createTime = createTime;
    }
}

