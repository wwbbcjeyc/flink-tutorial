package com.zitd.example;

import com.zitd.example.utils.MySource3;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * 在电商网站买了商品，订单完成之后，如果用户24小时之内没评论，系统自动好评。
 * 我们通过flink的定时器来简单的实现这个功能
 */
public class AutoEvaluation {
    private static final Logger LOG = LoggerFactory.getLogger(AutoEvaluation.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource3());
        //经过interval毫秒用户未对订单做出评价，自动给与好评.
        //我们为了演示方便，设置了5s的时间
        long interval = 5000l;
        dataStream.keyBy(0).process(new TimerProcessFuntion(interval));
        env.execute();
    }

    private static class TimerProcessFuntion extends KeyedProcessFunction<Tuple,Tuple2<String,Long>,Object> {

        private MapState<String,Long> mapState;
        //超过多长时间(interval,单位：毫秒) 没有评价，则自动五星好评
        private long interval = 0l;


        public TimerProcessFuntion(long interval) {
            this.interval=interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);



            MapStateDescriptor<String,Long> mapStateDesc = new MapStateDescriptor<>("mapStateDesc"
                    , String.class, Long.class);
            mapState =  getRuntimeContext().getMapState(mapStateDesc);
        }

        /**
         * 用户是否对该订单进行了评价，在生产环境下，可以去查询相关的订单系统.
         * 我们这里只是随便做了一个判断
         *
         * @param key
         * @return
         */
        private boolean isEvaluation(String key){
            return key.hashCode() % 2 == 0;
        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Object> out) throws Exception {
            mapState.put(value.f0, value.f1);
            ctx.timerService().registerProcessingTimeTimer(value.f1 + interval);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            super.onTimer(timestamp, ctx, out);

            Iterator iterator = mapState.iterator();
            while (iterator.hasNext()){
                Map.Entry<String,Long> entry = (Map.Entry<String,Long>) iterator.next();

                String orderid = entry.getKey();
                boolean f = isEvaluation(entry.getKey());
                mapState.remove(orderid);
                if (f){
                    LOG.info("订单(orderid: {}) 在  {} 毫秒时间内已经评价，不做处理", orderid, interval);
                }
                if (f){
                    //如果用户没有做评价，在调用相关的接口给与默认的五星评价
                    LOG.info("订单(orderid: {}) 超过  {} 毫秒未评价，调用接口给与五星自动好评", orderid, interval);
                }
            }


        }
    }
}
