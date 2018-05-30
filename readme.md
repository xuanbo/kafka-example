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