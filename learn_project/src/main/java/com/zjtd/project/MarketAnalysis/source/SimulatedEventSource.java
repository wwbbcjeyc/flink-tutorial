package com.zjtd.project.MarketAnalysis.source;

import com.zjtd.project.bean.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

// 定义一个自定义的模拟测试源
public class SimulatedEventSource extends RichParallelSourceFunction<MarketingUserBehavior> {

    // 定义是否运行的标识位
    private volatile boolean isRunning = true;

    // 定义渠道和用户行为的集合
    String channelSet[] = {
            "app-store", "huawei-store", "weibo", "wechat"
    };
    String behaviorSet[] = {
            "click", "download", "install", "uninstall"
    };
    private Random random = new Random();



    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
