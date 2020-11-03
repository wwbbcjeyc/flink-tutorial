package com.ztjd.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ztjd.util.ParseJsonData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncKuduTableRequest{

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        final SingleOutputStreamOperator<JSONObject> dataStream = dataStreamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });


        //dataStream.print();

        KuduAsyncFunction asyncFunction = new KuduAsyncFunction();

        SingleOutputStreamOperator<JSONObject> outputStreamOperator = AsyncDataStream.unorderedWait(dataStream
                , asyncFunction
                , 1000000L  //请求超时时间。
                , TimeUnit.MILLISECONDS
                , 20);

        outputStreamOperator.print();

        env.execute();

    }


    private static class KuduAsyncFunction extends RichAsyncFunction<JSONObject,JSONObject> {

        private transient ExecutorService executorService;
        private transient KuduClient kuduClient;
        private transient KuduSession kuduSession;
        private transient KuduTable kuduTable;
        private transient volatile Cache cache;
        private transient volatile KuduScanner scanner;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            kuduClient = new KuduClient.KuduClientBuilder("cdh001-cvm-bj3.qcloud.xiaobang.xyz").defaultAdminOperationTimeoutMs(60000).build();
            kuduSession = kuduClient.newSession();
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            kuduTable = kuduClient.openTable("impala::default.student");
            executorService = Executors.newFixedThreadPool(12);
            cache = CacheBuilder.newBuilder()
                    .concurrencyLevel(12) //设置并发级别 允许12个线程同时访问
                    .expireAfterAccess(2, TimeUnit.HOURS) //设置过期时间
                    .maximumSize(10000) //设置缓存大小
                    .build();

        }

        @Override
        public void close() throws Exception {
            super.close();
            kuduSession.close();
            kuduClient.close();
            ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService);
        }


        @Override
        public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
            executorService.submit(() ->{
                Long age = input.getLong("age");
                String sex = input.getString("sex");

                Schema schema = kuduTable.getSchema();
                KuduPredicate  predicate1 = KuduPredicate.newComparisonPredicate(schema.getColumn("age"), KuduPredicate.ComparisonOp.EQUAL, age);
                KuduPredicate  predicate2 = KuduPredicate.newComparisonPredicate(schema.getColumn("sex"), KuduPredicate.ComparisonOp.EQUAL, sex);

                List list = new ArrayList<String>();
                list.add("age");
                list.add("sex");

                scanner = kuduClient.newScannerBuilder(kuduTable).setProjectedColumnNames(list)
                        .addPredicate(predicate1)
                        .addPredicate(predicate2)
                        .build();

                Long sum = 0L;
                while (scanner.hasMoreRows()) {
                    RowResultIterator results = null;
                    try {
                        results = scanner.nextRows();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    int numRows = results.getNumRows();
                    sum +=numRows;
                }

                input.put("total",sum);
                resultFuture.complete(Collections.singleton(input));

            });
        }
    }



}
