package com.ztjd.table;

import com.ztjd.bean.WaterSensorTable03;
import com.ztjd.sinkTest.ClickHouseSink01;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Types;


/**
 * @Author wangwenbo
 * @Date 2020/11/23 1:01 上午
 * @Version 1.0
 */
public class SQLTableAPI03 {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 读取数据、转换
        SingleOutputStreamOperator<WaterSensorTable03> sensorDS = env
                .readTextFile("/Users/wangwenbo/Data/sensor-data.log")
                .map(new MapFunction<String, WaterSensorTable03>() {
                    @Override
                    public WaterSensorTable03 map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensorTable03(datas[0], String.valueOf(datas[1]), String.valueOf(datas[2]));

                    }
                });

        // TODO Table API
        // 1. 创建 表执行环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
               // .useOldPlanner() // 使用官方的 planner
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 把 DataStream 转换成 Table
        Table sensorTable = tableEnv.fromDataStream(sensorDS, "id,ts as timestamp,vc");

        // 3. 使用 Table API进行处理
        Table resultTable = sensorTable
                .select("id,vc");

        resultTable.printSchema();

        tableEnv.toAppendStream(resultTable,Row.class)
    .writeUsingOutputFormat(ClickHouseSink01.buildJDBCOutputFormat()
                .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
                .setDBUrl("jdbc:clickhouse://127.0.0.1:8123/default?socket_timeout=600000")
                .setUsername("")
                .setPassword("")
                .setQuery("insert into summtt01 values(?,?)")
                .setSqlTypes(new int[]{Types.CHAR,Types.CHAR})
                .setBatchInterval(1)
                .setMaxFlushTime(5000)
                .finish()).name("RetainsDetail.clickHouseSink").uid("RetainsDetail.clickHouseSink")
                .setParallelism(1).name("RetainsDetail.parallelism").uid("RetainsDetail.parallelism");

        // 4. 保存到 本地文件系统
        // 连接外部系统，将外部系统抽象成 一个 Table对象，需要指定存储格式、表的结构信息（字段名、类型）、表名
        // 第一步 connect() 外部系统的连接描述器，官方有 FS、Kafka、ES
        // 第二步 withFormat  指定 外部系统 数据的存储格式
        // 第三步 withSchema 要抽象成的 Table 的 Schema信息，有 字段名、字段类型
        // 第四步 createTemporaryTable 给抽象成的 Table 一个 表名
        /*tableEnv
                .connect(new FileSystem().path("out/flink.txt"))
                .withFormat(new OldCsv().fieldDelimiter("|"))
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("hahaha", DataTypes.INT())
                )
                .createTemporaryTable("fsTable");*/

        // 使用 TableAPI里的 insertInto, 把一张表的数据 插入到 另一张表(外部系统抽象成的 Table)
        //resultTable.insertInto("fsTable");

        env.execute();
    }
}
