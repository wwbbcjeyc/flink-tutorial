package com.zitd.example.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MySource implements SourceFunction<Tuple2<String,Double>> {

    private volatile boolean isRunning = true;
    private Random random = new Random();
    String category[] = {
            "女装", "男装",
            "图书", "家电",
            "洗护", "美妆",
            "运动", "游戏",
            "户外", "家具",
            "乐器", "办公"
    };

    @Override
    public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
        while (isRunning){
            Thread.sleep(10);
            //某一个分类
            String c = category[(int) (Math.random() * (category.length - 1))];
            //某一个分类下产生了price的成交订单
            double price = random.nextDouble() * 100;
            ctx.collect(Tuple2.of(c, price));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
