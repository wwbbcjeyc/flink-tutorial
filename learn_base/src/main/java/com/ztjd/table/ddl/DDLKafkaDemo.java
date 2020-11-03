package com.ztjd.table.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DDLKafkaDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        String  createSource=
                "create table kafka_source\n" +
                        "(\n" +
                        "name varchar,\n" +
                        "city varchar\n" +
                        ")with (\n" +
                        "'connector.type' = 'kafka', \n" +
                        "'connector.version' = 'universal',\n" +
                        "'connector.topic' = 'test',\n" +
                        "'connector.properties.0.key' = 'group.id',\n" +
                        "'connector.properties.0.value' = 'test_gd',\n" +
                        "'connector.properties.1.key' = 'bootstrap.servers',\n" +
                        "'connector.properties.1.value' = '127.0.0.1:9092',\n" +
                        "'connector.property-version' = '1',\n" +
                        "'connector.startup-mode' = 'earliest-offset',\n" +
                        "'format.type' = 'json',\n" +
                        "'format.property-version' = '1',\n" +
                        "'format.derive-schema' = 'true',\n" +
                        "'update-mode' = 'append')";

        tableEnv.sqlUpdate(createSource);

        String createSink= "create table kafka_sink \n" +
                "(\n" +
                "name varchar,\n" +
                "city varchar\n" +
                ")with (\n" +
                "'connector.type' = 'kafka', \n" +
                "'connector.version' = 'universal',\n" +
                "'connector.topic' = 't_rs',\n" +
                "'connector.properties.0.key' = 'group.id',\n" +
                "'connector.properties.0.value' = 'test_gd',\n" +
                "'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "'connector.properties.1.value' = '127.0.0.1:9092',\n" +
                "'connector.property-version' = '1',\n" +
                "'format.type' = 'json',\n" +
                " 'connector.sink-partitioner'= 'fixed' , \n"+
                "'format.property-version' = '1',\n" +
                "'format.derive-schema' = 'true',\n" +
                "'update-mode' = 'append')";

        tableEnv.sqlUpdate(createSink);

        tableEnv.sqlUpdate("INSERT INTO  `kafka_sink`  select * from `kafka_source`");

        tableEnv.execute("test");

    }
}
