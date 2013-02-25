package com.datasift.dropwizard.kafka;

import com.datasift.dropwizard.kafka.config.KafkaProducerConfiguration;
import com.datasift.dropwizard.kafka.producer.AsyncProducer;
import com.datasift.dropwizard.zookeeper.config.ZooKeeperConfiguration;
import com.yammer.dropwizard.config.Environment;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducerFactory<K,V> {

    private final Environment environment;

    public KafkaProducerFactory(Environment environment) {
        this.environment = environment;
    }

    public AsyncProducer<K, V> build(KafkaProducerConfiguration configuration) {
        Producer<K, V> producer = new Producer<K, V>(toProducerConfig(configuration));
        AsyncProducer<K, V> asyncProducer = new AsyncProducer<K, V>(producer);
        return asyncProducer;
    }

    private ProducerConfig toProducerConfig(KafkaProducerConfiguration configuration) {
        final ZooKeeperConfiguration zookeeper = configuration.getZookeeper();
        final Properties props = new Properties();

        props.setProperty("zk.connect",
                zookeeper.getQuorumSpec());
        props.setProperty("zk.connectiontimeout.ms",
                String.valueOf(zookeeper.getConnectionTimeout().toMilliseconds()));
        props.setProperty("zk.sessiontimeout.ms",
                String.valueOf(zookeeper.getSessionTimeout().toMilliseconds()));
        props.setProperty("buffer.size",
                String.valueOf(configuration.getSendBufferSize().toBytes()));
        props.setProperty("connect.timeout.ms",
                String.valueOf(configuration.getConnectionTimeout().toMilliseconds()));
        props.setProperty("reconnect.interval",
                String.valueOf(configuration.getReconnectInterval()));
        props.setProperty("max.message.size",
                String.valueOf(configuration.getMaxMessageSize().toBytes()));
        props.setProperty("compression.codec",
                String.valueOf(configuration.getCompression().getCodec().codec()));
        props.setProperty("compressed.topics",
                String.valueOf(configuration.getCompressedTopics()));
        props.setProperty("zk.read.num.retries",
                String.valueOf(configuration.getPartitionMissRetries()));
        props.setProperty("queue.time",
                String.valueOf(configuration.getAsync().getQueueTime().toMilliseconds()));
        props.setProperty("queue.size",
                String.valueOf(configuration.getAsync().getQueueSize()));
        props.setProperty("batch.size",
                String.valueOf(configuration.getAsync().getBatchSize()));

        return new ProducerConfig(props);
    }
}
