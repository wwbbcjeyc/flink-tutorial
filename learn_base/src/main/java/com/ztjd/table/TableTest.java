package com.ztjd.table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.*;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

import java.util.stream.Stream;

public class TableTest {
    public static void main(String[] args) throws Exception {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建老版本流查询环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment TableEnv = StreamTableEnvironment.create(env, settings);

        //创建老版本批式查询环境
         ExecutionEnvironment barchEnv = ExecutionEnvironment.getExecutionEnvironment();
         BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(barchEnv);

         //创建blink 版本的流查询环境
         EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        // StreamTableEnvironment bsTableEnv = StreamTableEnvironment. create(env, bsSettings);

         //创建blink 批式查询环境
         EnvironmentSettings bbSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

       //  TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

         //在环境中注册表,从外部系统读取数据
         //连接到文件系统(csv)
        String filePath = "/Users/wangwenbo/IdeaProjects/flink-tutorial/learn_base/src/main/resources/sensor.txt";

        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv()) //定义读取数据后的格式化方法
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE())

                )//定义表结构.
        .createTemporaryTable("inputTable"); //注册一张表
        //使用新的标准版Csv



        //转化成流打印输出
        Table sensorTable = tableEnv.from("inputTable");
        tableEnv.toAppendStream(sensorTable, TypeInformation.of(new TypeHint<Tuple3<String, Long,Double>>(){})).print();


        env.execute("Table api test");






    }
}
