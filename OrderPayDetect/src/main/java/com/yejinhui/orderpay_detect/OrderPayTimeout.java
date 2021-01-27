package com.yejinhui.orderpay_detect;

import com.yejinhui.orderpay_detect.beans.OrderEvent;
import com.yejinhui.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @Date 2021/1/23 16:31
 * @Created by huijinye@126.com
 */
public class OrderPayTimeout {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据转换成pojo类型
        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventDataStream = env.readTextFile(resource.getPath()).map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] fields = value.split(",");
                return new OrderEvent(Long.valueOf(fields[0]), String.valueOf(fields[1]),
                        String.valueOf(fields[2]), Long.valueOf(fields[3]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getTimestamp() * 1000;
            }
        });

        //1.定义一个带时间限制的模式，15分钟之内支付订单就行
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        //2.定义侧输出流标签，用来表示超时时间未支付的订单
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout") {
        };

        //3.将pattern应用到输入数据流上，得到pattern stream
        PatternStream<OrderEvent> pattern = CEP.pattern(orderEventDataStream.keyBy(new KeySelector<OrderEvent, Long>() {
            @Override
            public Long getKey(OrderEvent value) throws Exception {
                return value.getOrderId();
            }
        }), orderPayPattern);

        //4.调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = pattern.select(orderTimeoutTag,
                new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print("pay normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect job");
    }

    //实现自定义的事件处理函数
    private static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            Long orderId = pattern.get("create").get(0).getOrderId();
            return new OrderResult(orderId, "timeout " + timeoutTimestamp);
        }
    }

    //实现自定义的正常匹配事件处理函数
    private static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long payOrderId = pattern.get("pay").get(0).getOrderId();
            return new OrderResult(payOrderId, "payed");
        }
    }
}
