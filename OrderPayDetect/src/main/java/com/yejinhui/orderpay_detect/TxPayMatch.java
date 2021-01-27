package com.yejinhui.orderpay_detect;

import com.yejinhui.orderpay_detect.beans.OrderEvent;
import com.yejinhui.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.net.URL;

/**
 * @Date 2021/1/25 15:48
 * @Created by huijinye@126.com
 */
public class TxPayMatch {

    //定义侧输出流标签
    //pay来了，receipt没来
    private static final OutputTag<OrderEvent> unmatchedPays =
            new OutputTag<OrderEvent>("unmatched-pays") {
            };

    //receipt来了，pay没来
    private static final OutputTag<ReceiptEvent> unmatchedReceipts =
            new OutputTag<ReceiptEvent>("unmatched-receipts") {
            };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取订单支付事件数据
        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventDataStream = env.readTextFile(orderResource.getPath()).map(new MapFunction<String, OrderEvent>() {
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
        }).filter(new FilterFunction<OrderEvent>() {
            //交易id不为空，必须是pay事件
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return !"".equals(value.getTxId());
            }
        });

        //读取到账事件数据
        URL receiptResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
        DataStream<ReceiptEvent> receiptEventDataStream = env.readTextFile(receiptResource.getPath()).map(new MapFunction<String, ReceiptEvent>() {
            @Override
            public ReceiptEvent map(String value) throws Exception {
                String[] fields = value.split(",");
                return new ReceiptEvent(fields[0], fields[1], Long.valueOf(fields[2]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent element) {
                return element.getTimestamp() * 1000;
            }
        });

        //将两条流进行连接合并，进行匹配处理，如果不匹配的事件输出到侧输出流
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventDataStream.keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent value) throws Exception {
                return value.getTxId();
            }
        }).connect(receiptEventDataStream.keyBy(new KeySelector<ReceiptEvent, String>() {
            @Override
            public String getKey(ReceiptEvent value) throws Exception {
                return value.getTxId();
            }
        })).process(new TxPayMatchDetect());

        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmathced-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmathced-receipts");

        env.execute("tx match detect job");
    }

    //实现自定义的CoProcessFunction
    private static class TxPayMatchDetect extends CoProcessFunction<OrderEvent, ReceiptEvent,
            Tuple2<OrderEvent, ReceiptEvent>> {

        //定义状态，保存当前已经到来的订单支付事件和到账事件
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
            receiptState = getRuntimeContext().getState(
                    new ValueStateDescriptor<ReceiptEvent>("receipt", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //订单支付事件来了，判断是否已经有对应的到账事件
            ReceiptEvent receiptEvent = receiptState.value();
            if (receiptEvent != null) {
                //如果receipt不为空，说明到账事件已经来过，输出匹配事件，清空状态
                out.collect(new Tuple2<OrderEvent, ReceiptEvent>(pay, receiptEvent));
                payState.clear();
                receiptState.clear();
            } else {
                //如果receipt没来，设置定时器，更新支付状态
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp() + 5) * 1000); //等待对账数据5s
                payState.update(pay);
            }
        }

        @Override
        public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //到账事件来了，判断是否已经有对应的订单支付事件
            OrderEvent orderEvent = payState.value();
            if (orderEvent != null) {
                //如果pay不为空，说明订单支付事件已经来过，输出匹配事件，清空状态
                out.collect(new Tuple2<OrderEvent, ReceiptEvent>(orderEvent, receipt));
                payState.clear();
                receiptState.clear();
            } else {
                //如果pay没来，设置定时器，更新到账状态
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp() + 3) * 1000); //等待支付数据3s
                receiptState.update(receipt);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //定时器触发，有可能是有一个事件没来，不匹配；也可能是两个事件都来了，已经输出并清空状态
            OrderEvent orderEvent = payState.value();
            ReceiptEvent receiptEvent = receiptState.value();
            if (orderEvent != null) {
                //说明订单支付的来了，到账的没来
                ctx.output(unmatchedPays, orderEvent);
            }
            if (receiptEvent != null) {
                //说明订单支付没来，到账的来了
                ctx.output(unmatchedReceipts,receiptEvent);
            }

            //清空状态
            payState.clear();
            receiptState.clear();
        }
    }
}
