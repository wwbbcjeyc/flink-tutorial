package com.ztjd.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 关于异步IO要关注的点，主要是：
 * 1.有序IO的API。orderedWait请求的顺序和返回的顺序一致。
 * 2.无序IO的API。unorderedWait，主要是请求元素的顺序与返回元素的顺序不保证一致。
 */
public class AsyncIOSideTableJoinRedis {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        //dataStreamSource.print();
        SingleOutputStreamOperator<String> dataStream = dataStreamSource.map(x -> {
            JSONObject jsonObject = JSON.parseObject(x);
            //System.out.println(jsonObject.toJSONString());
            return jsonObject.toJSONString();
        }).returns(Types.STRING);

        SampleAsyncFunction asyncFunction = new SampleAsyncFunction();
        final SingleOutputStreamOperator<String> outputStreamOperator = AsyncDataStream.unorderedWait(dataStream
                , asyncFunction
                , 1000000L  //请求超时时间。
                , TimeUnit.MILLISECONDS
                , 20 //同时运行的最大异步请求数。
        );

        outputStreamOperator.print();




        // add async operator to streaming job
        DataStream<String> result;

       /* if (true) {
            result = AsyncDataStream.orderedWait(
                    dataStreamSource,
                    asyncFunction,
                    1000000L,
                    TimeUnit.MILLISECONDS,
                    20).setParallelism(1);
        }
        else {
            result = AsyncDataStream.unorderedWait(
                    dataStreamSource,
                    asyncFunction,
                    10000,
                    TimeUnit.MILLISECONDS,
                    20).setParallelism(1);
        }

        result.print();*/
        env.execute();



    }

    private static class SampleAsyncFunction extends RichAsyncFunction<String,String> {

        private transient RedisClient redisClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            RedisOptions config = new RedisOptions();
            config.setHost("127.0.0.1");
            config.setPort(6379);

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);//设置支持的EventLoop线程的数量
            vo.setWorkerPoolSize(20); //设置支持的Worker线程的最大数量

            Vertx vertx = Vertx.vertx(vo);
            redisClient = RedisClient.create(vertx, config);

        }

        @Override
        public void close() throws Exception {
            super.close();
            if(redisClient!=null)
                redisClient.close(null);
        }

        @Override
        public void asyncInvoke(String inputs, ResultFuture<String> resultFuture) throws Exception {

             JSONObject input = JSON.parseObject(inputs);
            String key = input.getString("key");

            // 获取hash-key值
           // redisClient.hget(fruit,"hash-key",getRes->{});
            redisClient.scard(key,getRes->{
                if(getRes.succeeded()){
                     Long result = getRes.result();
                     if(result == null){
                         resultFuture.complete(null);
                         return;
                     }else{
                         input.put("total",result);
                         resultFuture.complete(Collections.singleton(input.toJSONString()));
                     }
                }else if(getRes.failed()){
                    resultFuture.complete(null);
                    return;
                }
            });

            // 直接通过key获取值，可以类比
            /*redisClient.get(fruit,getRes->{
                if(getRes.succeeded()){
                    String result = getRes.result();
                    if(result== null){
                        resultFuture.complete(null);
                        return;
                    }else {
                        input.put("docs",result);
                        resultFuture.complete(Collections.singleton(input.toJSONString()));
                    }
                }else if(getRes.failed()){
                    resultFuture.complete(null);
                    return;
                }
            });*/
        }


    }
}
