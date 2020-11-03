package com.ztjd.sinkTest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

public class ClickHouseSink  extends RichSinkFunction<String> {
    String address = "jdbc:clickhouse://172.16.200.18:8123/default";
    Connection connection = null;
    PreparedStatement statement = null;
    //FlightDate Date,Year UInt16,name String,city String
    String sql = "insert into `flink_test` (FlightDate,Year,name,city) values (?,?,?,?)";
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        connection = DriverManager.getConnection(address);

    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        String[] split = value.split(",");
        String flightDate =split[0];
        String year =split[1];
        String name =split[2];
        String city =split[3];

        statement  = connection.prepareStatement(sql);
        statement.setDate(1,new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(flightDate).getTime()));
        statement.setInt(2,Integer.parseInt(year));
        statement.setString(3,name);
        statement.setString(4,city);
        statement.execute();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 8888);
        source.addSink(new ClickHouseSink());
        env.execute("test");
    }
}
