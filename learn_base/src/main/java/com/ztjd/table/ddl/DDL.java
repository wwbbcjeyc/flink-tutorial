package com.ztjd.table.ddl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DDL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,setting);
        String createTable=
                "CREATE TABLE user_info (" +
                        " name VARCHAR COMMENT '姓名'," +
                        " age VARCHAR COMMENT '年龄')" +
                        " WITH ( " +
                        " 'connector.type' = 'kafka'," +
                        " 'connector.version' = '0.10'," +
                        " 'connector.topic' = 'sensor',"+
                        " 'connector.startup-mode' = 'latest-offset',"+
                        " 'connector.properties.0.key' = 'bootstrap.servers',"+
                        " 'connector.properties.0.value' = '10.20.20.23:9092',"+
                        " 'update-mode' = 'append',"+
                        " 'format.type' = 'json',"+
                        " 'format.derive-schema' = 'true'"+
                        ") ";
        tableEnv.sqlUpdate(createTable);
        String query="SELECT name,age FROM user_info ";
        Table table = tableEnv.sqlQuery(query);
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        rowDataStream.print();
        env.execute("test");


    }
}
