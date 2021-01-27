package com.yejinhui.orderpay_detect;

import com.yejinhui.orderpay_detect.beans.OrderEvent;
import com.yejinhui.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * @Date 2021/1/25 15:48
 * @Created by huijinye@126.com
 */
public class TxPayMatchByJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取订单支付事件数据
        URL orderResource = TxPayMatchByJoin.class.getResource("/OrderLog.csv");
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
        URL receiptResource = TxPayMatchByJoin.class.getResource("/ReceiptLog.csv");
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

        //区间连接两条流，得到匹配的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventDataStream.keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent value) throws Exception {
                return value.getTxId();
            }
        }).intervalJoin(receiptEventDataStream.keyBy(new KeySelector<ReceiptEvent, String>() {
            @Override
            public String getKey(ReceiptEvent value) throws Exception {
                return value.getTxId();
            }
        })).between(Time.seconds(-3), Time.seconds(5))//[-3,5]区间范围
                .process(new TxPayMatchDetectByJoin());

        resultStream.print();

        env.execute("tx pay match by join job");
    }

    //实现自定义ProcessJoinFunction
    private static class TxPayMatchDetectByJoin extends
            ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        @Override
        public void processElement(OrderEvent left, ReceiptEvent right,
                                   Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left,right));
        }
    }
}
