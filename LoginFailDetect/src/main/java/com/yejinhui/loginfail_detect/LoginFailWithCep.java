package com.yejinhui.loginfail_detect;

import com.yejinhui.loginfail_detect.beans.LoginEvent;
import com.yejinhui.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * CEP（complex event processing）实现登录失败检测
 *
 * @Date 2021/1/22 16:41
 * @Created by huijinye@126.com
 */
public class LoginFailWithCep {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");
        DataStream<String> dataStream = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<LoginEvent> loginEventStream = dataStream.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] fields = value.split(",");
                return new LoginEvent(Long.valueOf(fields[0]),
                        fields[1], fields[2], Long.valueOf(fields[3]));
            }
        }).assignTimestampsAndWatermarks( //Watermark定义：在这之前的数据都到齐了
                new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        //CEP模式处理
        //1.定义一个事件发生的匹配模式
        //firstFail -> secondFail,within 2s
        Pattern<LoginEvent, LoginEvent> loginFailPattern0 = Pattern.<LoginEvent>
                begin("firstFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("secondFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("thirdFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).within(Time.seconds(3));

        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>
                begin("failEvents").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
            //consecutive()意味着连续
        }).times(3).consecutive().within(Time.seconds(5));

        //2.将匹配模式用到事件流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(new KeySelector<LoginEvent, Long>() {
            @Override
            public Long getKey(LoginEvent value) throws Exception {
                return value.getUserId();
            }
        }), loginFailPattern);

        //3.检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream =
                patternStream.select(new LoginFailMatchDetectWarning());


        warningStream.print();

        env.execute("login fail detect with cep job");
    }

    //实现自定义的PatternSelectFunction
    private static class LoginFailMatchDetectWarning implements
            PatternSelectFunction<LoginEvent, LoginFailWarning> {

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
//            LoginEvent firstFailEvent = pattern.get("firstFail").iterator().next();
//            LoginEvent thirdFailEvent = pattern.get("thirdFail").get(0);
//            return new LoginFailWarning(firstFailEvent.getUserId(),firstFailEvent.getTimestamp(),
//                    thirdFailEvent.getTimestamp(),"login fail 3 times");

            LoginEvent firstFailEvent = pattern.get("failEvents").get(0);
            LoginEvent lastFailEvent = pattern.get("failEvents").get(pattern.get("failEvents").size() - 1);
            return new LoginFailWarning(firstFailEvent.getUserId(), firstFailEvent.getTimestamp(),
                    lastFailEvent.getTimestamp(), "login fail " + pattern.get("failEvents").size() + " times");
        }

    }

}
