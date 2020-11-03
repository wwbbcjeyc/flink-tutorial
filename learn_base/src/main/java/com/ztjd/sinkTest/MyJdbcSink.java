package com.ztjd.sinkTest;

import com.ztjd.bean.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MyJdbcSink extends RichSinkFunction<SensorReading> {

    private Connection conn;
    private PreparedStatement insertStmt;
    private PreparedStatement updataStmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test","root","123456");
        insertStmt = conn.prepareStatement("insert into sensor_tmp(sensor,temperature) value (?,?) ;");
        updataStmt = conn.prepareStatement("update sensor_tmp set temperature = ? where sensor =? ;");
    }

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        updataStmt.setDouble(1,value.getTemperature());
        updataStmt.setString(2,value.getId());
        updataStmt.execute();
        if(updataStmt.getUpdateCount() ==0){
            insertStmt.setString(1,value.getId());
            insertStmt.setDouble(2,value.getTemperature());
            insertStmt.execute();
        }

    }

    @Override
    public void close() throws Exception {
        if (insertStmt != null) {
            updataStmt.close();
        }

        if (insertStmt != null) {
            insertStmt.close();
        }

        if (conn != null) {
            conn.close();
        }

    }
}
