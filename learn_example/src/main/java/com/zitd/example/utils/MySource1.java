package com.zitd.example.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySource1 implements SourceFunction<Tuple2<String,Integer>> {


    private volatile boolean isRunning = true;
    String category[] = {"Android", "IOS", "H5"};
    @Override
    public void run(SourceContext ctx) throws Exception{
        while (isRunning){
            Thread.sleep(10);
            //具体是哪个端的用户
            String type = category[(int) (Math.random() * (category.length))];
            //随机生成10000以内的int类型数据作为userid
            int userid = (int) (Math.random() * 10000);
            ctx.collect(Tuple2.of(type, userid));
        }
    }
    @Override
    public void cancel(){
        isRunning = false;
    }
}
