package com.yejinhui.networkflow_analysis;

import com.yejinhui.networkflow_analysis.beans.PageViewCount;
import com.yejinhui.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

/**
 * @Date 2021/1/20 8:58
 * @Created by huijinye@126.com
 */
public class PageView {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据，创建DataStream
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 转换为UserBehavior，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserBehavior(Long.valueOf(fields[0])
                        , Long.valueOf(fields[1])
                        , Integer.valueOf(fields[2])
                        , fields[3]
                        , Long.valueOf(fields[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            // 转换成毫秒，这样就获取到了带有时间标记的数据流
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 分组开窗聚合，得到每个窗口内各个商品的count值
        // 过滤pv行为
        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream0 = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        }).map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return new Tuple2<>("pv", 1L);
            }
        })
                .keyBy(0)
                .timeWindow(Time.hours(1))   // 1小时的滚动窗口
                .sum(1);

        // 并行任务改进，设计随机key，解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        }).map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                Random random = new Random();
                return new Tuple2<>(random.nextInt(10), 1L);
            }
        }).keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
                return value.f0;
            }
        }).timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 将各个数据分区的结果汇总起来
        DataStream<PageViewCount> pvResultStream = pvStream.keyBy(new KeySelector<PageViewCount, Long>() {
            @Override
            public Long getKey(PageViewCount value) throws Exception {
                return value.getWindowEnd();
            }
        }).process(new TotalPvCount()); //等到所有数据收集完毕
//        .sum("count"); // 来数据就统计

        pvResultStream.print();

        env.execute("pv count job");
    }

    // 实现自定义预聚合函数
    private static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator + 1L;
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

    // 实现自定义窗口函数
    private static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {

        @Override
        public void apply(Integer integer, TimeWindow window,
                          Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(integer.toString(),
                    window.getEnd(), input.iterator().next()));
        }
    }

    // 实现自定义处理函数，把相同窗口分组统计count值叠加
    private static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {

        // 定义状态，保存当前的总count值
        ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            totalCountState.update(totalCountState.value() + value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCountState.value()));
            // 触发完毕清空状态
            totalCountState.clear();
        }
    }
}
