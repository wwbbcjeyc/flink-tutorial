package com.ztjd.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;

public class StateStrategies {

    public static void main(String[] args) {

         ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

         //固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,  //任务失败后重启次数
                Time.seconds(10) //任务失败10秒后需要开始执行重启操作
        ));

       //失败率重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, //两次尝试重启之间时间间隔
                Time.minutes(5), //计算失败率的时间间隔
                Time.seconds(10) //任务认定为失败之前，最大的重启次数
        ));

    }
}
