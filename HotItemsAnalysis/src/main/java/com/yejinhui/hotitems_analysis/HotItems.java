package com.yejinhui.hotitems_analysis;

import com.yejinhui.hotitems_analysis.beans.ItemViewCount;
import com.yejinhui.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Date 2021/1/15 11:16
 * @Created by huijinye@126.com
 */
public class HotItems {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\study\\idea-workspace\\atguigu\\bigdata\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "consumer-group");
//        properties.setProperty("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        // 重置偏移量从最近的开始
//        properties.setProperty("auto.offset.reset", "latest");
//        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems",
//                new SimpleStringSchema(),properties));

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
        DataStream<ItemViewCount> windowAggStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        })
                // 按照商品id分组
                .keyBy("itemId")
                // 开滑动窗口
                .timeWindow(Time.minutes(60), Time.minutes(5))
                // 聚合操作
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 收集同一窗口的所有商品count数据，排序输出topN；为了统计每个窗口下最热门的商品，需要再次按窗口分组
        DataStream<String> resultStream = windowAggStream.keyBy("windowEnd")  //按照窗口分组
                .process(new TopNHotItems(5));//用自定义处理函数排序取前5

        resultStream.print();

        env.execute("hot items analysis");
    }

    // 实现自定义增量聚合，预聚合，减少state压力，统计窗口中的条数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    // 自定义全窗口函数，将每个key的每个窗口聚合后的结果带上，这样就得到了每个商品在每个窗口的点击量的数据流
    private static class WindowItemCountResult implements WindowFunction<Long,
            ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input,
                          Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            long windowEnd = window.getEnd();
            // 迭代器中只有一个数
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    // 实现自定义的KeyedProcessFunction
    private static class TopNHotItems extends KeyedProcessFunction<Tuple,
            ItemViewCount, String> {

        // 定义属性，top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("item-view-count-list",
                            ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx,
                                   Collector<String> out) throws Exception {
            // 每来一条数据都会调用此方法
            // 存入list，注册定时器
            itemViewCountListState.add(value);
            // 窗口延迟1ms，防止有晚到的数据；注册eventTime定时器，触发定时器会调用onTimer()
            // 注册一个windowEnd+1的定时器(Flink框架会自动忽略同一时间的重复注册)
            // windowEnd+1的定时器被触发时，意味着收到了windowEnd+1的WaterMark = 收集齐了该windowEnd下所有商品窗口统计值
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        // 触发定时器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集所有的数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts =
                    Lists.newArrayList(itemViewCountListState.get().iterator());

            // 倒排序
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 将信息格式化成string，打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("==========================");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1))
                    .append("\n");

            // 遍历数据，取出topN
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(itemViewCount.getItemId())
                        .append(" 热门度 = ").append(itemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("==========================\n\n");

            //控制输出频率，因为当前读的是文件
            TimeUnit.SECONDS.sleep(1);

            out.collect(resultBuilder.toString());
        }
    }
}
