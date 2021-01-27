package com.yejinhui.networkflow_analysis;

import com.yejinhui.networkflow_analysis.beans.ApacheLogEvent;
import com.yejinhui.networkflow_analysis.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.job.savepoints.SavepointHandlers;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @Date 2021/1/18 15:10
 * @Created by huijinye@126.com
 */
public class HotPages {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取文件，转成pojo
//        URL resource = HotPages.class.getResource("/apache.log");
//        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 将一行log日志转成一个pojo
        DataStream<ApacheLogEvent> dataStream = inputStream.map(
                new MapFunction<String, ApacheLogEvent>() {
                    @Override
                    public ApacheLogEvent map(String value) throws Exception {
                        String[] fields = value.split(" ");
                        SimpleDateFormat simpleDateFormat =
                                new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        // 毫秒单位
                        Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                        return new ApacheLogEvent(fields[0], fields[1], timestamp,
                                fields[5], fields[6]);
                    }
                }).assignTimestampsAndWatermarks(
                // 因为近来的数据是乱序的，所以使用BoundedOutOfOrdernessTimestampExtractor
                // 等待1s中输出近似正确的结果，到了触发窗口时间，在等1s晚到数据进来在计算，等待时间太长就会导致时效差
                new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });
        dataStream.print("data");


        // 分组开窗聚合
        // 定义一个侧输出流标签，必须和原数据类型一致；这种迟到的数据只能走离线批处理了
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
        };

        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream.filter(
                new FilterFunction<ApacheLogEvent>() {
                    // 只统计get请求
                    @Override
                    public boolean filter(ApacheLogEvent value) throws Exception {
                        return "GET".equals(value.getMethod());
                    }
                }).filter(new FilterFunction<ApacheLogEvent>() {
            // url中包含css|js|png|ico不参与统计
            @Override
            public boolean filter(ApacheLogEvent value) throws Exception {
                String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                return Pattern.matches(regex, value.getUrl());
            }
        }).keyBy(new KeySelector<ApacheLogEvent, String>() {
            @Override
            public String getKey(ApacheLogEvent value) throws Exception {
                return value.getUrl();
            }
            // 窗口长度10分钟，滑动5s
        }).timeWindow(Time.minutes(10), Time.seconds(5))
                // 允许迟到1minute的数据，来了迟到的数据就更新结果
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag) //漏网之鱼的数据，输出到侧输出流
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowAggStream.print("agg");
        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count数据，排序输出；按照窗口结束时间进行keyby
        DataStream<String> resultStream = windowAggStream.keyBy(
                new KeySelector<PageViewCount, Long>() {
                    @Override
                    public Long getKey(PageViewCount value) throws Exception {
                        return value.getWindowEnd();
                    }
                })
                .process(new TopNHotPages(3));

        resultStream.print();
        env.execute("hot pages job");
    }

    // 预聚合
    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    // 结果统计
    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(s, window.getEnd(), input.iterator().next()));
        }
    }

    // 自定义处理函数
    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

        // 定义属性，top N的大小
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，存储PageViewCount
//        ListState<PageViewCount> pageViewCountListState;
        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountListState = getRuntimeContext().getListState(
//                    new ListStateDescriptor<PageViewCount>("page-view-count-list",
//                            PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>
                    ("page-view-count-map", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，都会调用此方法
            // 存入list，注册定时器
//            pageViewCountListState.add(value);
            pageViewCountMapState.put(value.getUrl(), value.getCount());

            // 窗口延迟1ms，防止有晚到的数据；注册eventTime定时器，就会触发onTimer()
            // 注册windowEnd+1定时器被触发时，意味着收到了windowEnd+1的WaterMark = 收集齐了windowsEnd+1的所有页面被点击的数据
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

            // 因为.allowedLateness(Time.minutes(1))，所以1minute触发定时器，清空MapState
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60 * 1000L);
        }

        // 触发定时器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 判断是否到了窗口关闭清理时间，如果是，直接清空MapState，结束
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                pageViewCountMapState.clear();
                return;
            }

            // 如果注册的是过时的定时器是不会被直接触发的，会在下一次watermark更新的时候触发
            // 定时器触发，当前已收集所有数据，倒序输出
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(
                    pageViewCountMapState.entries());

            // 按照count倒排
            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o1.getValue() < o2.getValue()) {
                        return 1;
                    } else if (o1.getValue() > o2.getValue()) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            });

            // 将信息格式化成string，输出
            StringBuilder stringBuilder = new StringBuilder("=========================");
            stringBuilder.append("窗口结束时间：")
                    .append(new Timestamp(timestamp - 1))
                    .append("\n");

            // 遍历输出，取出topN
            for (int i = 0; i < Integer.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> pageViewCount = pageViewCounts.get(i);
                stringBuilder.append("No ").append(i + 1).append(":")
                        .append(" 页面url = ").append(pageViewCount.getKey())
                        .append(" 浏览次数 = ").append(pageViewCount.getValue())
                        .append("\n");
            }

            stringBuilder.append("\n\n");
            TimeUnit.SECONDS.sleep(1);
            out.collect(stringBuilder.toString());


            //清空pageViewCounts
//            pageViewCountListState.clear();
        }
    }

}
