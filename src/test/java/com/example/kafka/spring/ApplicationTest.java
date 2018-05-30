package com.example.kafka.spring;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

/**
 * @author xuan
 * @since 1.0.0
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class ApplicationTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);

    @Autowired
    private KafkaTemplate<String, String> template;

    @Test
    public void send() throws IOException {
        this.template.send("test", "foo1");
        this.template.send("test", "foo2");
        this.template.send("test", "foo3");
        System.in.read();
    }

    @KafkaListener(topics = "test")
    public void listen(ConsumerRecord<String, String> record) {
        LOG.info(record.toString());
    }
}
