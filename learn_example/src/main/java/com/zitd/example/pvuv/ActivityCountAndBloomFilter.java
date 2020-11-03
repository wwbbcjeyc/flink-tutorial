package com.zitd.example.pvuv;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ActivityCountAndBloomFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //u001,A1,2019-09-02 10:10:11,1,北京市 数据
        DataStreamSource<String> lines = env.socketTextStream("127.0.0.1", 7777);

        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> tpDataStream =
                lines.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {

                    @Override
                    public Tuple5<String, String, String, String, String> map(String lines) throws Exception {

                        String[] spArr = lines.split(",");
                        String uid = spArr[0];
                        String act = spArr[1];
                        String dt = spArr[2];
                        String type = spArr[3];
                        String province = spArr[4];

                        return Tuple5.of(uid, act, dt, type, province);
                    }
                });

        KeyedStream<Tuple5<String, String, String, String, String>, Tuple> keyedStream =
                tpDataStream.keyBy(1, 3);

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> result =
                keyedStream.process(new KeyedProcessFunction<Tuple, Tuple5<String, String, String, String, String>,
                                        Tuple4<String, String, Integer, Integer>>() {

                    //保存分组数据去重后用户ID的布隆过滤器
                    private transient ValueState<BloomFilter> bloomState = null;
                    //保存去重后总人数的state，加transient禁止参与反序列化
                    private transient ValueState<Integer> uvCountState = null;
                    //保存活动的点击数的state
                    private transient ValueState<Integer> pvCountState = null;


                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<BloomFilter> bloomDescriptor = new ValueStateDescriptor<>(
                                "blomm-filter",
                                // 数据类型的class对象，因为是布隆过滤器，所有要使用这种方式来拿
                                TypeInformation.of(new TypeHint<BloomFilter>() {
                                })
                        );
                        ValueStateDescriptor<Integer> uvCountDescriptor = new ValueStateDescriptor<>(
                                "uv-count",
                                Integer.class
                        );
                        ValueStateDescriptor<Integer> pvCountDescriptor = new ValueStateDescriptor<>(
                                "pv-count",
                                Integer.class
                        );

                        bloomState = getRuntimeContext().getState(bloomDescriptor);
                        uvCountState = getRuntimeContext().getState(uvCountDescriptor);
                        pvCountState = getRuntimeContext().getState(pvCountDescriptor);
                    }


                    @Override
                    public void processElement(Tuple5<String, String, String, String, String> input, Context ctx,
                                               Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {

                        String uid = input.f0;
                        String actId = input.f1;
                        String type = input.f3;

                        //将值取出来，因为ValueState中实质上是以特殊的map集合存储的，一个key,一个value
                        BloomFilter bloomFilter = bloomState.value();
                        Integer uvCound = uvCountState.value();
                        Integer pvCount = pvCountState.value();

                        //初始化上面三个变量
                        if (pvCount == null) {
                            pvCountState.update(1);
                        }else{
                            pvCountState.update(pvCount +1);
                        }
                        if (bloomFilter == null) {
                            bloomFilter =  BloomFilter.create(Funnels.unencodedCharsFunnel(),10000000);
                            uvCound = 0;
                        }
                        //布隆过滤器中是否不包含uid,是的话就返回false
                        if (!bloomFilter.mightContain(uid)) {
                            bloomFilter.put(uid); //不包含就添加进去
                            uvCountState.update(uvCound+1);
                        }


                        bloomState.update(bloomFilter);



                        out.collect(Tuple4.of(actId, type, pvCountState.value(), uvCountState.value()));
                    }
                });

        result.print();
        env.execute("ActivityCountAndBloomFilter");




    }
}
