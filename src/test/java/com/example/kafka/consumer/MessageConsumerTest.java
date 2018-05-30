package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * 消息消费者-手动提交
 *
 * @author xuan
 * @since 1.0.0
 */
public class MessageConsumerTest {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerTest.class);

    public static void main(String[] args) {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.104.131.255:9092");
        // group
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅Topic
        consumer.subscribe(Collections.singletonList("test"));

        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>(minBatchSize);
        // 获取消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    buffer.add(record);
                }

                if (buffer.size() >= minBatchSize) {
                    // insertIntoDb(buffer);
                    // 手动提交
                    consumer.commitSync();
                    // 清空buffer
                    buffer.clear();
                }
            }
        } finally {
            consumer.close();
        }
    }

}
