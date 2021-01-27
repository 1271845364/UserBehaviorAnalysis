package com.yejinhui.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @Date 2021/1/18 10:23
 * @Created by huijinye@126.com
 */
public class KafkaProducerUtil {

    public static void main(String[] args) throws IOException {
        writeToKafka("hotitems");
    }

    /**
     * 高并发写入kafka
     *
     * @param topic
     * @throws IOException
     */
    public static void writeToKafka(String topic) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 缓冲方式读取文本数据
        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\study\\idea-workspace\\atguigu\\bigdata\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            kafkaProducer.send(new ProducerRecord<String, String>(topic, line));

        }
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
        if (bufferedReader != null) {
            bufferedReader.close();
        }
    }
}
