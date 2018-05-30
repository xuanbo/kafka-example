# 读我

kafka学习代码。

## 基础

代码：

* 消费者：`com.example.kafka.consumer`
* 生产者：`com.example.kafka.producer`

其中消费者均为单线程消费，分为手动提交、自动提交两个版本。

在生产下，肯定是多消费者（不管是多线程还是多进程）。

## 多线程

### 生产者

由于生产者是线程安全的，因此，更推荐一个生产者实例，多线程引用进行生产的方式。这种方式通常要比每个线程维护一个KafkaProducer实例效率要高。

### 消费者

对于KafkaConsumer而言，它不是线程安全的，所以实现多线程时通常由两种实现方法：

* 每个线程维护一个KafkaConsumer
* 维护一个或多个KafkaConsumer，并用一个任务线程池处理任务

优缺点分析：

方法一：实现简单（即把之前的消费对象new几个就完了）。但是消费者线程处理任务容易超时，导致rebalance。
方法二：实现难度相对大一点，扩展性好。但是任务处理链路变长，消费者已经commit了offset，但是任务还是任务线程池处理中，任务处理失败如何处理是个问题。


这两种方法或是模型都有各自的优缺点，个人觉得第二种更好，即不要将很重的处理逻辑放入消费者的代码中，很多Kafka consumer使用者碰到的各种rebalance超时、coordinator重新选举、心跳无法维持等问题都来源于此。

第二种方案代码：`com.example.kafka.consumer.MessageConsumerMultiThreadTest`

**根据实际情况修改Handler和线程池的初始化。**

## 注意

kafka的吞吐量与partition有关，一个partition只能给一个消费者消费，因此，partition数量要多于消费者。

否则，会有消费者分配不到partition而浪费资源。

## spring kafka源码分析

核心：
`org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#processCommits`

消费者poll时，默认500条。

将500条record消费，然后每消费一条，记录到ack中，之后遍历，加到offsets中，最后commits。

在debug过程中，看源码时间太长，导致session超时了，因此这500条commit offset失败，发生了rebalance。

```java
org.apache.kafka.clients.consumer.CommitFailedException: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records.
	at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.sendOffsetCommitRequest(ConsumerCoordinator.java:725)
	at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.commitOffsetsSync(ConsumerCoordinator.java:604)
	at org.apache.kafka.clients.consumer.KafkaConsumer.commitSync(KafkaConsumer.java:1173)
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.commitIfNecessary(KafkaMessageListenerContainer.java:1180)
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.processCommits(KafkaMessageListenerContainer.java:1049)
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.run(KafkaMessageListenerContainer.java:625)
	at java.util.concurrent.Executors$RunnableAdapter.call$$$capture(Executors.java:511)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java)
	at java.util.concurrent.FutureTask.run$$$capture(FutureTask.java:266)
	at java.util.concurrent.FutureTask.run(FutureTask.java)
	at java.lang.Thread.run(Thread.java:745)
```

这个时候，就发生了rebalance。

下面是Stack Overflow上面大神的[分析](https://stackoverflow.com/questions/43991845/kafka10-1-heartbeat-interval-ms-session-timeout-ms-and-max-poll-interval-ms)：

* `session.timeout.ms` is for heartbeat thread. If coordinator fails to get any heartbeat from a consumer before this time interval elapsed, it marks consumer as failed and triggers a new round of rebalance.

* `max.poll.interval.ms` is for user thread. If message processing logic is too heavy to cost larger than this time interval, coordinator explicitly have the consumer leave the group and also triggers a new round of rebalance.

* `heartbeat.interval.ms` is used to have other healthy consumers aware of the rebalance much faster. If coordinator triggers a rebalance, other consumers will only know of this by receiving the heartbeat response with `REBALANCE_IN_PROGRESS` exception encapsulated. Quicker the heartbeat request is sent, faster the consumer knows it needs to rejoin the group.

Suggested values:
* `session.timeout.ms`: a relatively low value, 10 seconds for instance.
* `max.poll.interval.ms`: based on your processing requirements
* `heartbeat.interval.ms`: a relatively low value, better 1/3 of the session.timeout.ms

注意：`heartbeat.interval.ms` This was added in 0.10.1 and it will send heartbeat between polls.

因此，这也告诉了我们，要快速poll，否则容易超时，出现rebalance问题。如果程序真的500条在规定的session时长中消费不完，那么可以调整参数`max.poll.records`(0.9后才有效)、或者调节`max.poll.interval.ms`，`session.timeout.ms`参数（具体看官网）。

例如，我的spring boot kafka配置如下：
```properties
#============== kafka ===================
# 指定kafka 代理地址，可以多个
spring.kafka.bootstrap-servers=47.104.131.255:9092

# =============== producer ===============

spring.kafka.producer.retries=0
# 每次批量发送消息的数量
spring.kafka.producer.batch-size=16384
spring.kafka.producer.buffer-memory=33554432

# 指定消息key和消息体的编解码方式
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# =============== consumer ===============
# 指定默认消费者group id
spring.kafka.consumer.group-id=test

# spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false
# spring.kafka.consumer.auto-commit-interval=100

# 每次poll的条数
spring.kafka.consumer.max-poll-records=200

# 指定消息key和消息体的编解码方式
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
```
这句配置就指定了poll的记录条数：`spring.kafka.consumer.max-poll-records=200`

在实际情况中，要根据处理业务的时间，来调节相应的参数。