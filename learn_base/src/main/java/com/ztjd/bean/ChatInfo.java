package com.ztjd.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author wangwenbo
 * @Date 2020/11/25 12:49 上午
 * @Version 1.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChatInfo {

    private String userId;
    private int eventTime;
    private int sales;

    public static String convertToCsv(ChatInfo ChatInfo) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");

        builder.append("'");
        builder.append(ChatInfo.getUserId());
        builder.append("', ");

        builder.append(String.valueOf(ChatInfo.getEventTime()));
        builder.append(", ");

        builder.append(String.valueOf(ChatInfo.getSales()));
        builder.append(" )");

        return builder.toString();
    }

}
