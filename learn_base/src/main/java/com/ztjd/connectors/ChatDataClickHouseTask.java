package com.ztjd.connectors;

import com.alibaba.fastjson.JSONObject;
import com.ztjd.bean.ChatInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;



public class ChatDataClickHouseTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.2.109:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer_group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>("sensor", new SimpleStringSchema(), properties);
        DataStreamSource<String> streamSource = env.addSource(consumer010);
        //streamSource.print();

        ResourceBundle rb = ResourceBundle.getBundle("clickhouse");

        String targeTableName =  rb.getString(ClickhouseSinkConsts.TARGET_TABLE_NAME+".sensor");
        String house =  rb.getString(ClickhouseClusterSettings.CLICKHOUSE_HOSTS);
        String user =  rb.getString(ClickhouseClusterSettings.CLICKHOUSE_USER);
        String pass =  rb.getString(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD);
        String size =  rb.getString(ClickhouseSinkConsts.MAX_BUFFER_SIZE);

        Properties clickhouseProps = new Properties();
        clickhouseProps.put(ClickhouseSinkConsts.TARGET_TABLE_NAME, targeTableName);
        clickhouseProps.put(ClickhouseSinkConsts.MAX_BUFFER_SIZE, size);

        Map<String, String> globalParameters = new HashMap<>();

        // clickhouse cluster properties
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, house);
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_USER, user);
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, pass);

        // sink common
        globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, "10");
        globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, "");
        globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, "10");
        globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, "10");
        globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, "10");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);

        env.getConfig().setGlobalJobParameters(parameters);

        streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {

                ChatInfo chatInfo1 = new ChatInfo();
                JSONObject jsonObject = JSONObject.parseObject(s);

                chatInfo1.setUserId(jsonObject.getString("sensorId").trim());
                chatInfo1.setEventTime(jsonObject.getInteger("ts"));
                chatInfo1.setSales(jsonObject.getInteger("sale"));


                return ChatInfo.convertToCsv(chatInfo1);
            }
        }).addSink(new ClickhouseSink(clickhouseProps)).name("clickhouse sink");


        env.execute("ch-test");



    }
}
