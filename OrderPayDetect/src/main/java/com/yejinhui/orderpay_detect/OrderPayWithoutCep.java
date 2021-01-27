package com.yejinhui.orderpay_detect;

import com.yejinhui.orderpay_detect.beans.OrderEvent;
import com.yejinhui.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @Date 2021/1/25 10:02
 * @Created by huijinye@126.com
 */
public class OrderPayWithoutCep {

    //定义超时事件的侧输出流标签
    private static final OutputTag<OrderResult> orderTimeoutTag =
            new OutputTag<OrderResult>("order-timeout") {
            };

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

        //自定义处理函数：主流输出正常匹配的支付订单(15分钟内支付的)，侧输出流输出超时的订单，标签定义成全局的
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventDataStream.keyBy(new KeySelector<OrderEvent, Long>() {
            @Override
            public Long getKey(OrderEvent value) throws Exception {
                return value.getOrderId();
            }
        }).process(new OrderPayMatchDetect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");
    }

    //实现自定义的KeyedProcessFunction
    private static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        //定义状态，保存之前订单是否已经来过create、pay的事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;

        //定义状态，保存定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
            isCreatedState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("is-created", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            //先获取当前状态
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timerTs = timerTsState.value();

            //判断当前事件类型
            if ("create".equals(value.getEventType())) {
                //1.防止数据乱序，比如pay比create数据先到了
                if (isPayed) {
                    //1.1如果已经支付了，输出正常结果
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully1111111"));
                    //清空状态，删除定时器
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    //1.2如果没支付过，注册15minute后的定时器，开启等待pay事件
                    Long ts = (value.getTimestamp() + 15 * 60) * 1000;
                    ctx.timerService().registerEventTimeTimer(ts);
                    //更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                //2.如果来的数据是pay，判断之前的是否create
                if (isCreated) {
                    //2.1已经创建过订单，继续判断当前的支付时间戳是否超过15分钟
                    if (value.getTimestamp() * 1000 < timerTs) {
                        //2.1.1在15minute之内支付的，正常匹配
                        out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    } else {
                        //2.1.2已经超过15分钟支付了，输出到侧输出流
                        ctx.output(orderTimeoutTag,
                                new OrderResult(value.getOrderId(), "payed but already timeout"));
                    }

                    //清空状态，删除定时器
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    //2.2没有创建过订单，说明数据是乱序的，注册定时器，等待创建订单事件到来
                    //注册的是创建订单的定时器
                    Long ts = value.getTimestamp() * 1000;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                    isPayedState.update(true);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            //定时器触发，说明一定有一个(create or pay)事件没来
            if (isPayedState.value()) {
                //pay来了，create没来
                ctx.output(orderTimeoutTag,
                        new OrderResult(ctx.getCurrentKey(), "payed but not found create log"));
            }
            if (isCreatedState.value()) {
                //create来了，pay没来
                ctx.output(orderTimeoutTag,
                        new OrderResult(ctx.getCurrentKey(), "timeout"));
            }

            //清空状态
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }
    }
}
