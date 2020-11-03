package com.ztjd.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ztjd.bean.DataLocation;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * TODO 读取 Kafka 数据,异步调用高德API,将经纬度转换成省份输出
 *
 * log日志格式：100001,Lucy,2020-02-14 13:28:27,116.310003,39.991957
 *
 * 高德API:
 * https://restapi.amap.com/v3/geocode/regeo?output=json&location=116.310003,39.991957&key=<你申请的key></>&radius=1000&extensions=all
 *
 */

public class AsyncQueryAmapLocation {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group");
        //如果没有记录偏移量，第一次从最开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        final FlinkKafkaConsumer010<String> consumer011 = new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaDataStream = env.addSource(consumer011);

        final SingleOutputStreamOperator<DataLocation> dataStream = AsyncDataStream.unorderedWait(kafkaDataStream, new AsyncDatabaseRequest()
                , 0, TimeUnit.MILLISECONDS, 10);


    }

    private static class AsyncDatabaseRequest extends RichAsyncFunction<String, DataLocation> {

        private transient CloseableHttpAsyncClient httpAsyncClient = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            //初始化异步的HttpClient
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(3000) //设置Socket超时时间
                    .setConnectTimeout(3000) //设置连接超时时间
                    .build();

            httpAsyncClient =  HttpAsyncClients.custom()
                    .setMaxConnTotal(20) //设置最大连接数
                    .setDefaultRequestConfig(requestConfig).build();

            httpAsyncClient.start();

        }

        /**
         * 完成对 Kafka 中读取到的数据的操作
         * @param input
         * @param resultFuture
         * @throws Exception
         */
        @Override
        public void asyncInvoke(String input, ResultFuture<DataLocation> resultFuture) throws Exception {
            //100001,Lucy,2020-02-14 13:28:27,116.310003,39.991957
            String[] fields = input.split(",");
            //id
            String id = fields[0];
            //name
            String name = fields[1];
            //date
            String date = fields[2];
            //longitude
            String longitude = fields[3];
            //latitude
            String latitude = fields[4];

            String amap_url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+longitude+","+latitude+"&key=<你申请的key>&radius=1000&extensions=all";
            // Execute request

             HttpGet httpGet = new HttpGet(amap_url);
             Future<HttpResponse> execute = httpAsyncClient.execute(httpGet, null);
             //设置回调在客户端完成请求后执行
             //回调只是将结果转发到结果的future
             CompletableFuture.supplyAsync(new Supplier<String>() {
                @Override
                public String get() {
                    try {
                        HttpResponse response = execute.get();
                        String province = null;
                        if (response.getStatusLine().getStatusCode() == 200) {
                            //获取请求的Json字符串
                            String result = EntityUtils.toString(response.getEntity());
                            //将Json字符串转成Json对象
                            JSONObject jsonObject = JSON.parseObject(result);
                            //获取位置信息
                            JSONObject regeocode = jsonObject.getJSONObject("regeocode");

                            if (regeocode != null && !regeocode.isEmpty()) {
                                JSONObject address = regeocode.getJSONObject("addressComponent");
                                //获取省份信息
                                province = address.getString("province");
                            }
                        }
                        return province;
                    } catch (Exception e) {
                        // Normally handled explicitly.
                        return null;
                    }

                }
            }).thenAccept( (String dbResult) -> {
                 resultFuture.complete(Collections.singleton(DataLocation.of(id,name,date,dbResult)));
             });
        }

        @Override
        public void close() throws Exception {
            super.close();
            httpAsyncClient.close();
        }

    }
}
