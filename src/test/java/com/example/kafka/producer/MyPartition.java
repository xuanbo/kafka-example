package com.example.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author xuan
 * @since 1.0.0
 */
public class MyPartition implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(MyPartition.class);

    public MyPartition() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int partitionNum;
        try {
            partitionNum = Integer.parseInt((String) key);
        } catch (Exception e) {
            partitionNum = key.hashCode();
        }
        LOG.debug("the message sendTo topic:" + topic + " and the partitionNum:" + partitionNum);
        return Math.abs(partitionNum % numPartitions);
    }

    @Override
    public void close() {
    }

}
