package com.ztjd.table.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DDLKafkaToMysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

         EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

         StreamTableEnvironment tabEnv = StreamTableEnvironment.create(bsEnv, settings);



        String  sourceSql=
                "create table kafka_source\n" +
                        "(\n" +
                        "name varchar,\n" +
                        "city varchar\n" +
                        "proctime as PROCTIME() "+
                        ")with (\n" +
                        "'connector.type' = 'kafka', \n" +
                        "'connector.version' = 'universal',\n" +
                        "'connector.topic' = 'sensor',\n" +
                        "'connector.properties.0.key' = 'group.id',\n" +
                        "'connector.properties.0.value' = 'consumer_group',\n" +
                        "'connector.properties.1.key' = 'bootstrap.servers',\n" +
                        "'connector.properties.1.value' = '10.20.20.23:9092',\n" +
                        "'connector.property-version' = '1',\n" +
                        "'connector.startup-mode' = 'earliest-offset',\n" +
                        "'format.type' = 'json',\n" +
                        "'format.property-version' = '1',\n" +
                        "'format.derive-schema' = 'true',\n" +
                        "'update-mode' = 'append')";



         tabEnv.sqlQuery(sourceSql).printSchema();

       /* String mysqlsql = "CREATE TABLE pv (\n" +
                "  day_str STRING,\n" +
                "  pv bigINT,\n" +
                "  PRIMARY KEY (day_str) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'table-name' = 'pv'\n" +
                ")";

        
        tabEnv.sqlQuery(mysqlsql);

        tabEnv.sqlQuery("insert into pv SELECT DATE_FORMAT(proctime, 'yyyy-MM-dd') as day_str, count(*) \n" +
                "FROM datagen \n" +
                "GROUP BY DATE_FORMAT(proctime, 'yyyy-MM-dd')");*/

        tabEnv.execute("kafkaToMysql");


    }
}
