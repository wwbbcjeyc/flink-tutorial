package com.ztjd.table;

import com.ztjd.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class TableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.readTextFile("/Users/wangwenbo/IdeaProjects/flink-tutorial/learn_base/src/main/resources/sensor.txt");
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) input -> {
            String[] dataArray = input.split(",");
            return new SensorReading(dataArray[0], Long.valueOf(dataArray[1]), Double.valueOf(dataArray[2]));
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //基于数据流转换成一张表 操作
        Table dataTable = tableEnv.fromDataStream(dataStream);

         //调用table api 获得转换结果
         Table resultTable = dataTable.select("id,temperature")
                .filter("id =='sensor_1'");

         // 直接用sql实现
        tableEnv.createTemporaryView("dataTable", dataTable);
        String sql = "select id, temperature from dataTable where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        //转换回数据流，打印输出
        DataStream<Tuple2<String, Double>> resultStream = tableEnv.toAppendStream(resultTable, Types.TUPLE(Types.STRING,Types.DOUBLE));
        //resultStream.print("result");
        DataStream<Tuple2<String, Double>> resultSqlStream = tableEnv.toAppendStream(resultSqlTable, Types.TUPLE(Types.STRING,Types.DOUBLE));
        resultTable.printSchema();
        resultSqlStream.print("sqlResult");


       env.execute("table example job");
    }
}
