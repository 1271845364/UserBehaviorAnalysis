package com.yejinhui.market_analysis;

import com.yejinhui.market_analysis.beans.AdClickEvent;
import com.yejinhui.market_analysis.beans.AdCountViewByProvince;
import com.yejinhui.market_analysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

/**
 * 区分省份统计广告
 *
 * @Date 2021/1/21 14:10
 * @Created by huijinye@126.com
 */
public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //元数据结构：userId,adId,province,city,timestamp
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        DataStream<String> dataStream = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<AdClickEvent> adClickEventStream = dataStream.map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String value) throws Exception {
                String[] fields = value.split(",");
                return new AdClickEvent(Long.valueOf(fields[0]), Long.valueOf(fields[1])
                        , fields[2], fields[3], Long.valueOf(fields[4]));
            }
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        //对同一个用户点击同一个广告的行为进行检测、黑名单报警，使用ProcessFunction
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventStream.keyBy("userId", "adId")   //基于用户id和广告id做分组
                .process(new FilterBlackListUser(100));

        //基于省份分组，开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = filterAdClickStream.keyBy(new KeySelector<AdClickEvent, String>() {
            @Override
            public String getKey(AdClickEvent value) throws Exception {
                return value.getProvince();
            }
        })
                .timeWindow(Time.hours(1), Time.minutes(5))  //滑动窗口
                .aggregate(new AdCountAgg(), new AdCountResult());//来一条就count一次，连同window信息包装在一起

        adCountResultStream.print();
        //获取侧输出流
        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist") {
        }).print("blacklist-user");

        env.execute("ad count by province job");
    }

    private static class AdCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            out.collect(new AdCountViewByProvince(s, windowEnd, input.iterator().next()));
        }
    }

    private static class FilterBlackListUser extends KeyedProcessFunction<
            Tuple, AdClickEvent, AdClickEvent> {

        //点击次数上限
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        //定义状态，保存当前用户对某一个广告的点击次数
        ValueState<Long> countState;

        //标志状态，保存当前用户是否已经被发送到了黑名单里面
        ValueState<Boolean> isSendState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("ad-count", Long.class, 0L));
            isSendState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("is-send", Boolean.class, false));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            //判断当前用户对同一广告的点击次数，如果不够上限，就count+1，正常输出；如果达到了上限就过滤掉
            // ，输出黑名单报警

            //获取当前count值
            Long curCount = countState.value();

            //1.判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器；state中的数据要每隔一天就清空，不能无限增大
            if (curCount == 0) {
                // ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) = 1970年到现在多少天
                //(ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * 24 * 60 * 60 * 1000 = 伦敦的第二天0点
                Long ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * 24 * 60 * 60 * 1000 - 8 * 60 * 60 * 1000;
                ctx.timerService().registerEventTimeTimer(ts);
            }

            //2.判断是否报警
            if (curCount >= countUpperBound) {
                //判断是否输出到黑名单过，如果没有就输出报警
                if (!isSendState.value()) {
                    //更新状态
                    isSendState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist") {
                               },
                            new BlackListUserWarning(value.getUserId(), value.getAdId(),
                                    "click over " + countUpperBound + " times"));

                }
                //达到了上限就过滤掉
                return;
            }
            //如果没有返回，点击次数+1，更新countState，正常输出当前数据到主流
            countState.update(curCount + 1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            //定时器触发，清空状态
            countState.clear();
            isSendState.clear();
        }
    }

}
