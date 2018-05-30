package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/**
 * 消息消费者-自动提交
 *
 * @author xuan
 * @since 1.0.0
 */
public class MessageConsumerAutoCommitTest {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerAutoCommitTest.class);

    public static void main(String[] args) {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.104.131.255:9092");
        // group
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅Topic
        consumer.subscribe(Collections.singletonList("test"));

        // 获取消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                LOG.info("topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

}
