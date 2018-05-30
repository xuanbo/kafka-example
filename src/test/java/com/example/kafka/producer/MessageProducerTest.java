package com.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 消息生产者
 *
 * @author xuan
 * @since 1.0.0
 */
public class MessageProducerTest {

    public static void main(String[] args) {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.104.131.255:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 设置属性 自定义分区类
        props.put("partitioner.class", "com.example.kafka.producer.MyPartition");

        // Producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Send
        for (int i = 0; i < 10000; i++) {
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)));
        }

        // Close
        producer.close();
    }

}
