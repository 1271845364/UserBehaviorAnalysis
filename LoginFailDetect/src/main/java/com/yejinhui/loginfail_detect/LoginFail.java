package com.yejinhui.loginfail_detect;

import com.yejinhui.loginfail_detect.beans.LoginEvent;
import com.yejinhui.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Date 2021/1/22 9:11
 * @Created by huijinye@126.com
 */
public class LoginFail {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFail.class.getResource("/LoginLog.csv");
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

        //按照userId分组聚合
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream.keyBy(new KeySelector<LoginEvent, Long>() {
            @Override
            public Long getKey(LoginEvent value) throws Exception {
                return value.getUserId();
            }
        }).process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute("login fail detect job");
    }

    //因为前面先keyBy(userId)，只对当前的userId有效，不同的用户之间是不会影响的
    private static class LoginFailDetectWarning0 extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        //最大连续登陆失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning0(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        //定义状态，保存2s内所有的登陆失败事件
        ListState<LoginEvent> loginFailEventListState;

        //为了删除定时器；保存注册的定时器的时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class)
            );
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer-ts", Long.class)
            );
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            //判断当前登录事件类型
            if ("fail".equals(value.getLoginState())) {
                //1.如果是登录失败事件，添加到列表状态中
                loginFailEventListState.add(value);
                //如果没有注册定时器，注册一个2s之后的定时器
                if (timerTsState.value() == null) {
                    Long ts = (value.getTimestamp() + 2) * 1000;
                    ctx.timerService().registerEventTimeTimer(ts);
                    //更新timerTsState
                    timerTsState.update(ts);
                }
            } else {
                //2.如果登录成功，删除定时器，清空状态，重新开始
                if (timerTsState.value() != null) {
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                    //清空状态
                    timerTsState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            //定时器触发，说明2s内没有登录成功的数据，判断listState中登录失败的个数
            ArrayList<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get());
            Integer failTimes = loginFailEvents.size();

            if (failTimes > maxFailTimes) {
                //如果超出设定的登录失败的最大次数，就报警
                Long firstFailTime = loginFailEvents.get(0).getTimestamp();
                Long lastFailTime = loginFailEvents.get(failTimes - 1).getTimestamp();
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        firstFailTime, lastFailTime,
                        "login fail in 2s for " + failTimes + " times"));
            }

            //定时器触发完毕，就需要清空状态
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }

    //这中方式存在一个问题，乱序的数据会影响中间的处理流程
    private static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

        //最大连续登陆失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        //定义状态，保存2s内所有的登陆失败事件
        ListState<LoginEvent> loginFailEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class)
            );
        }

        //以登录事件作为判断报警的触发条件，不在注册定时器
        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            //判断当前登录事件类型
            if ("fail".equals(value.getLoginState())) {
                //1.如果是登录失败，获取状态中之前的登录失败事件，继续判断是否有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    //1.1连续失败次数超过了最大失败次数限制，报警
                    Long firstFailTime = iterator.next().getTimestamp();
                    Long lastFailTime = value.getTimestamp();
                    //连着两次登录失败时间间隔2s以内
                    if (lastFailTime - firstFailTime <= 2) {
                        out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                                firstFailTime, lastFailTime,
                                "login fail 2 times in 2s"));
                        //清空状态，放入最新的登录失败事件
                        loginFailEventListState.clear();
                        loginFailEventListState.add(value);
                    } else {
                        //清空状态，放入最新的登录失败事件
                        loginFailEventListState.clear();
                        loginFailEventListState.add(value);
                    }
                } else {
                    //1.2如果没有达到最大的连续失败次数，就把当前的事件放入loginFailEventListState
                    loginFailEventListState.add(value);
                }
            } else {
                //2.登录成功，清空状态
                loginFailEventListState.clear();
            }
        }
    }
}
