package com.yejinhui.networkflow_analysis;

import com.yejinhui.networkflow_analysis.beans.PageViewCount;
import com.yejinhui.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;

/**
 * @Date 2021/1/20 15:19
 * @Created by huijinye@126.com
 */
public class UvWithBloomFilter {

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

        //
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        })
                // 并行度是1，不用分区计算，所以不用keyby；但是不推荐使用
                .timeWindowAll(Time.hours(1)) // 1小时的数据都丢到set中对userId去重，然后获取到的userId个数就是uv值
                // 来一条数据去redis中判断这条数据是否存在
                // 定义一个计算规则，每来一条数据立刻就触发一次窗口计算，在窗口计算的过程中连接redis，
                // 取出他的位图中保存的状态，然后判断userId是否在里面
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());

        uvStream.print();


        env.execute("uv count with bloom filter job");
    }

    // 自定义触发器
    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每一条数据来到，直接触发窗口计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // 清除一些自定义状态，如果没定义自定义状态，就不用管
        }
    }

    // 自定义布隆过滤器
    private static class MyBloomFilter {
        // 定义位图大小，一般定义2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 实现一个hash函数
        public Long hashCode(String value, Integer seed) {
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

    // 实现自定义的处理函数
    private static class UvCountResultWithBloomFilter extends
            ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        // jedis连接
        Jedis jedis;

        // 布隆过滤器
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            // 29 = (6[2的6次幂=64]+10[KB到MB，1024=2的10次幂]+10[B到KB]+3[2的3次幂=8,kb到KB])要处理1亿个数据，需要64MB大小的位图
            myBloomFilter = new MyBloomFilter(1 << 29);
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements,
                            Collector<PageViewCount> out) throws Exception {
            // 将位图和窗口count全部存入到redis，用windowEnd做key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();

            // 把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1.获取当前userId
            Long userId = elements.iterator().next().getUserId();

            // 2.计算位图中的offset，给个质数，61是质数
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 3.获取key的某一位，jedis.getbit()，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist) {
                // 如果不存在，对应位图位置设置为1
                jedis.setbit(bitmapKey, offset, true);

                // 更新redis中保存的count值
                String uvCountString = jedis.hget(countHashName, countKey);
                Long uvCount = 0L;
                // count有值
                if (uvCountString != null && !"".equals(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashName, countKey, String.valueOf((uvCount + 1)));

                out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

}
