package com.ztjd.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.ExecutorUtils;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class AsyncRedisKuduRequest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<JSONObject> dataStream = dataStreamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        ReidsKuduAsyncFunction reidsKuduAsync = new ReidsKuduAsyncFunction();


        SingleOutputStreamOperator<JSONObject> outputStreamOperator = AsyncDataStream.unorderedWait(dataStream
                , reidsKuduAsync
                , 1000000L  //请求超时时间。
                , TimeUnit.MILLISECONDS
                , 20);

        outputStreamOperator.print();

        env.execute();


    }

    private static class ReidsKuduAsyncFunction extends RichAsyncFunction<JSONObject,JSONObject> {


        private transient volatile JedisPool jedisPool;
        private transient Jedis jedis;

        private transient ExecutorService executorService;

        private transient KuduClient kuduClient;
        private transient KuduSession kuduSession;
        private transient KuduTable kuduTable;
        private transient volatile KuduScanner scanner;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            //Redis
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(12);  //最大连接数
            jedisPoolConfig.setMaxIdle(10);   //最大空闲
            jedisPoolConfig.setMinIdle(10);     //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true);  //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(500);//忙碌时等待时长 毫秒
            jedisPoolConfig.setTestOnBorrow(true); //每次获得连接的进行测试
           // jedisPool = new JedisPool(jedisPoolConfig, "172.16.200.45", 6379, Protocol.DEFAULT_TIMEOUT,"shX$t9TdiLjM4nJu");
            jedisPool = new JedisPool(jedisPoolConfig,"127.0.0.1",6379);
            jedis = jedisPool.getResource();
            jedis.select(1);

            //kudu
            kuduClient = new KuduClient.KuduClientBuilder("cdh001-cvm-bj3.qcloud.xiaobang.xyz").defaultAdminOperationTimeoutMs(60000).build();
            kuduSession = kuduClient.newSession();
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            kuduTable = kuduClient.openTable("impala::default.kudu_uuid");
            executorService = Executors.newFixedThreadPool(12);

        }



        @Override
        public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {

            executorService.submit(() ->{
                String event_sn = input.getString("event_sn");
                String flow_id = input.getString("flow_id");

                String key = event_sn+"_"+flow_id;

                if(!jedis.exists(key)){
                    Schema schema = kuduTable.getSchema();
                    KuduPredicate  predicate1 = KuduPredicate.newComparisonPredicate(schema.getColumn("event_sn"), KuduPredicate.ComparisonOp.EQUAL, event_sn);
                    KuduPredicate  predicate2 = KuduPredicate.newComparisonPredicate(schema.getColumn("flow_id"), KuduPredicate.ComparisonOp.EQUAL, flow_id);

                    List list = new ArrayList<String>();
                    list.add("event_sn");
                    list.add("flow_id");
                    list.add("uuid");

                    scanner = kuduClient.newScannerBuilder(kuduTable).setProjectedColumnNames(list)
                            .addPredicate(predicate1)
                            .addPredicate(predicate2)
                            .build();

                    Long sum = 0L;
                    while (scanner.hasMoreRows()) {
                        RowResultIterator results = null;
                        try {
                            results = scanner.nextRows();
                            while (results.hasNext()) {
                                RowResult result = results.next();
                                String uuid = result.getString(2);
                               jedis.sadd(key,uuid);
                            }
                            int numRows = results.getNumRows();
                            sum +=numRows;

                            //设置15天过期时间
                            LocalDateTime midnight = LocalDateTime.ofInstant(new Date().toInstant(),
                                    ZoneId.systemDefault()).plusDays(15).withHour(0).withMinute(0)
                                    .withSecond(0).withNano(0);

                            LocalDateTime currentDateTime = LocalDateTime.ofInstant(new Date().toInstant(),
                                    ZoneId.systemDefault());
                            jedis.expire(key,(int) ChronoUnit.SECONDS.between(currentDateTime, midnight));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }finally {
                            jedis.close();
                        }
                    }
                    input.put("total",sum);
                    resultFuture.complete(Collections.singleton(input));
                }else{
                    Long result = jedis.scard(key);
                    input.put("total",result);
                    resultFuture.complete(Collections.singleton(input));
                }
            });

        }


        @Override
        public void close() throws Exception {
            super.close();
            jedis.close();
            kuduSession.close();
            kuduClient.close();
            ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService);
        }



    }
}
