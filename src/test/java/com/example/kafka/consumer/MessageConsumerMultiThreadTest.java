package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消息消费者，多线程
 *
 * 实现方案：（一）多个消费者实例，一个任务线程池处理record。
 *
 * @author xuan
 * @since 1.0.0
 */
public class MessageConsumerMultiThreadTest {

    public static void main(String[] args) {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.104.131.255:9092");
        // group
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费组
        ConsumerGroup<String, String> consumerGroup = new ConsumerGroup<>(props, 5);
        // 订阅
        consumerGroup.subscribe(Collections.singletonList("test"));
        // 启动
        consumerGroup.start();

        // 这里主要是为了优雅停止，模拟30s停止线程池
        try {
            Thread.sleep(30000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        consumerGroup.shutdown();
    }

}

/**
 * 消费组，（一）多个消费者
 *
 * @param <K>
 * @param <V>
 */
class ConsumerGroup<K, V> {

    /**
     * 配置
     */
    private final Properties props;

    /**
     * 消费者数量
     */
    private final int num;

    /**
     * 消费者线程池
     */
    private ThreadPoolExecutor executor;

    /**
     * 任务线程池
     */
    private ThreadPoolExecutor handlerExecutor;

    /**
     * 消费者实例的封装
     */
    private List<ConsumerRunner<K, V>> consumerRunners;

    public ConsumerGroup(Properties props, int num) {
        this.props = props;
        this.num = num;
        // 线程池初始化，这里要跟根据具体业务定制
        executor = new ThreadPoolExecutor(num, num,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        handlerExecutor = new ThreadPoolExecutor(num, num,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        init();
    }

    /**
     * 初始化消费者
     */
    private void init() {
        String commit = props.getProperty("enable.auto.commit", "false");
        boolean autoCommit = Boolean.parseBoolean(commit);
        consumerRunners = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props);
            ConsumerRunner<K, V> consumerRunner = new ConsumerRunner<>(consumer, handlerExecutor, autoCommit);
            consumerRunners.add(consumerRunner);
        }
    }

    /**
     * 订阅主题
     *
     * @param topics 主题
     */
    public void subscribe(Collection<String> topics) {
        consumerRunners.forEach(consumerRunner -> consumerRunner.subscribe(topics));
    }

    /**
     * 启动
     */
    public void start() {
        consumerRunners.forEach(executor::execute);
    }

    /**
     * 关闭
     */
    public void shutdown() {
        // 消费者关闭
        consumerRunners.forEach(ConsumerRunner::shutdown);
        // 线程池关闭，优先关闭消费者线程池
        executor.shutdown();
        handlerExecutor.shutdown();
    }

}

/**
 * 消费者实例封装，poll数据到任务线程池
 *
 * @param <K>
 * @param <V>
 */
class ConsumerRunner<K, V> implements Runnable {

    /**
     * 是否关闭
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 消费者实例
     */
    private final KafkaConsumer<K, V> consumer;

    /**
     * 任务处理线程池
     */
    private final ThreadPoolExecutor handlerExecutor;

    /**
     * 自动提交
     */
    private final boolean autoCommit;

    public ConsumerRunner(KafkaConsumer<K, V> consumer, ThreadPoolExecutor handlerExecutor, boolean autoCommit) {
        this.consumer = consumer;
        this.handlerExecutor = handlerExecutor;
        this.autoCommit = autoCommit;
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                ConsumerRecords<K, V> records = consumer.poll(1000);
                for (ConsumerRecord<K, V> record : records) {
                    // 提交到任务线程池
                    handlerExecutor.execute(new ConsumerHandlerRunner<>(record));
                }
                if (!autoCommit) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 订阅topic
     *
     * @param topics 主题
     */
    public void subscribe(Collection<String> topics) {
        consumer.subscribe(topics);
    }

    /**
     * Shutdown hook which can be called from a separate thread
     */
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

}

/**
 * 处理任务，真正关注的业务逻辑实现
 *
 * @param <K>
 * @param <V>
 */
class ConsumerHandlerRunner<K, V> implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerHandlerRunner.class);

    private final ConsumerRecord<K, V> record;

    public ConsumerHandlerRunner(ConsumerRecord<K, V> record) {
        this.record = record;
    }

    @Override
    public void run() {
        // 处理业务逻辑
        LOG.info("record: {}", record);
    }

}
