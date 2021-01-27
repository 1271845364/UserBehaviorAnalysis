package com.yejinhui.market_analysis;

import com.yejinhui.market_analysis.beans.ChannelPromotionCount;
import com.yejinhui.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 全量统计
 *
 * @Date 2021/1/21 11:23
 * @Created by huijinye@126.com
 */
public class AppMarketingStatistics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1.从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = env.addSource(
                new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                });

        //2.开窗统计总量
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream.filter(new FilterFunction<MarketingUserBehavior>() {
            @Override
            public boolean filter(MarketingUserBehavior value) throws Exception {
                return !"UNINSTALL".equals(value.getBehavior());
            }
        }).map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                return new Tuple2<String, Long>("total", 1L);
            }
        }).keyBy(0)
                .timeWindow(Time.hours(1), Time.seconds(5)) // 定义滑动窗口
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        resultStream.print();

        env.execute("app marketing by channel job");

    }

    //实现自定义的模拟市场用户行为数据源
    private static class SimulatedMarketingUserBehaviorSource implements
            SourceFunction<MarketingUserBehavior> {

        //控制是否正常运行的标志位
        Boolean running = true;

        //定义用户行为和渠道范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            //只要是source执行起来，这个run方法就得一直运行，不断的发送数据
            while (running) {
                //随机生成所有字段
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();
                //发送数据
                ctx.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));
                //100ms发送一条数据，控制频率
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        @Override
        public void cancel() {
            //通过代码逻辑控制run方法结束，不在发送数据
            running = false;
        }
    }

    //自定义预聚合 增量聚合函数
    private static class MarketingStatisticsAgg implements
            AggregateFunction<Tuple2<String, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> tuple, Long accumulator) {
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

    //全窗口函数
    private static class MarketingStatisticsResult
            implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelPromotionCount> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new ChannelPromotionCount("total", "total", windowEnd, count));
        }
    }
}
