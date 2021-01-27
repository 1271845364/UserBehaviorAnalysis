package com.yejinhui.networkflow_analysis;

import com.yejinhui.networkflow_analysis.beans.PageViewCount;
import com.yejinhui.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

/**
 * @Date 2021/1/20 14:23
 * @Created by huijinye@126.com
 */
public class UniqueVisitor {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据，创建DataStream
        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
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

        // 开窗统计uv值
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        })
                // 并行度是1，不用分区计算，所以不用keyby；但是不推荐使用
                .timeWindowAll(Time.hours(1)) // 1小时的数据都丢到set中对userId去重，然后获取到的userId个数就是uv值
                .apply(new UvCountResult());

        uvStream.print();
        env.execute("uv count job");
    }

    // 自定义实现全窗口函数
    private static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values,
                          Collector<PageViewCount> out) throws Exception {
            // 定义一个set结构，保存窗口中的所有的userId，自动去重
            Set<Long> userIdSet = new HashSet<Long>();
            for (UserBehavior userBehavior : values) {
                userIdSet.add(userBehavior.getUserId());
            }
            out.collect(new PageViewCount("uv", window.getEnd(),
                    Long.valueOf(userIdSet.size())));
        }
    }
}
