package com.zitd.example.pvuv;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;

import java.io.Serializable;

public class PVTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 10080);

        SingleOutputStreamOperator<UserInfo> userStream = source.map(new MapFunction<String, UserInfo>() {
            @Override
            public UserInfo map(String s) throws Exception {
                String[] split = s.split(",");
                UserInfo subOrderDetail = new UserInfo();
                subOrderDetail.setOrderId(split[1]);
                subOrderDetail.setUserId(split[0]);
                return subOrderDetail;
            }
        });

        userStream
                .keyBy("userId")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .aggregate(new RsesultAggregateFunc())
                .print("每秒执行一次---------");
        env.execute("test");



    }

    private static class RsesultAggregateFunc implements AggregateFunction<UserInfo,ResultInfo,ResultInfo> {
        //初始化计数器：就是要给计算指标赋予初始值
        @Override
        public ResultInfo createAccumulator() {
            ResultInfo resultInfo = new ResultInfo();
            return resultInfo;
        }

        //指标结果合并
        @Override
        public ResultInfo merge(ResultInfo a, ResultInfo b) {
            b.setOrderId(a.getOrderId()+b.getOrderId());
            return b;
        }

        @Override
        public ResultInfo getResult(ResultInfo accumulator) {
            return accumulator;
        }

        //指标计算：指标增长的逻辑 比如pv userID一样加一
        @Override
        public ResultInfo add(UserInfo userInfo, ResultInfo resultInfo) {
            if(userInfo.getUserId().equals(resultInfo.getUserId())){
                resultInfo.setOrderId( resultInfo.count+=1);
            }else {
                resultInfo.setUserId(userInfo.getUserId());
                resultInfo.setOrderId(1);
            }
            return resultInfo;
        }
    }

    public static class UserInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private String userId;
        private String orderId;
        public UserInfo() {
        }
        public UserInfo(String userId, String orderId) {
            this.userId = userId;
            this.orderId = orderId;
        }
        public String getUserId() {
            return userId;
        }
        public void setUserId(String userId) {
            this.userId = userId;
        }
        public String getOrderId() {
            return orderId;
        }
        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }
    }
    public static class ResultInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private String userId;
        private int count;
        public ResultInfo() {
        }
        public ResultInfo(String userId, int count) {
            this.userId = userId;
            this.count = count;
        }
        public String getUserId() {
            return userId;
        }
        public void setUserId(String userId) {
            this.userId = userId;
        }
        public int getOrderId() {
            return count;
        }
        public void setOrderId(int count) {
            this.count = count;
        }
        @Override
        public String toString() {
            return   "ResultInfo{" +
                    "userId='" + userId + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
