package com.yejinhui.market_analysis;

import com.yejinhui.market_analysis.beans.ChannelPromotionCount;
import com.yejinhui.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 按照不同渠道app在市场的推广
 *
 * @Date 2021/1/21 10:00
 * @Created by huijinye@126.com
 */
public class AppMarketingByChannel {

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

        //2.分渠道开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream.filter(new FilterFunction<MarketingUserBehavior>() {
            @Override
            public boolean filter(MarketingUserBehavior value) throws Exception {
                return !"UNINSTALL".equals(value.getBehavior());
            }
        })
                // 组合分组
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

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
    private static class MarketingCountAgg implements
            AggregateFunction<MarketingUserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior value, Long accumulator) {
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
    private static class MarketingCountResult
            extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String timestamp = new Timestamp(context.window().getEnd()).toString();
            Long count = elements.iterator().next();
            out.collect(new ChannelPromotionCount(channel, behavior, timestamp, count));
        }
    }
}
