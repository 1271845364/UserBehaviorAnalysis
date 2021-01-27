package com.yejinhui.hotitems_analysis;

import com.yejinhui.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Date 2021/1/18 10:32
 * @Created by huijinye@126.com
 */
public class HotItemsWithSql {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2.设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3.读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\study\\idea-workspace\\atguigu\\bigdata\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        // 4.转换为UserBehavior，分配时间戳和watermark
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

        // 5.创建表执行环境，使用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 6.将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream,
                "itemId,behavior,timestamp.rowtime as ts");

        // 7.分组开窗
        Table windowAggTable = dataTable.filter("behavior = 'pv'")
                .window(Slide.over("1.hours")
                        .every("5.minutes")
                        .on("ts")
                        .as("w"))
                .groupBy("itemId,w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        // 8.利用开窗函数，对count值进行排序并获得row number，得到topN
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");

        // ROW_NUMBER()开窗函数去聚合
//        Table resultTable = tableEnv.sqlQuery("select * from (select *,ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num from agg) where row_num <= 5");

        // 纯SQL实现
        tableEnv.createTemporaryView("data_table",dataStream,"itemId, " +
                "behavior, timestamp.rowtime as ts");

        Table resultTable = tableEnv.sqlQuery("select * from (select *,ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num from " +
                "(select itemId, count(itemId) as cnt, HOP_END(ts,interval '5' minute, interval '1' hour) as windowEnd from data_table where behavior = 'pv' group by itemId, HOP(ts,interval '5' minute, interval '1' hour))" +
                ") where row_num <= 5");


        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute("hot items with sql job");
    }

}
