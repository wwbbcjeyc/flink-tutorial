package com.ztjd.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public  class SensorReading {

    private String id;
    private Long timestamp;
    private Double temperature;

    public static String convertToCsv(SensorReading Sensorreading) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");

        builder.append(Sensorreading.id);
        builder.append(" ,");

        builder.append("'");
        builder.append(Sensorreading.timestamp);
        builder.append("', ");



        builder.append("'");
        builder.append(Sensorreading.temperature);
        builder.append("' )");
        return builder.toString();
    }

}
