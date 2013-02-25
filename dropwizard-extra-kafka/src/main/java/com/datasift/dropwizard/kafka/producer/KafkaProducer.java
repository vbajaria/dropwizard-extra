package com.datasift.dropwizard.kafka.producer;

import kafka.javaapi.producer.ProducerData;

public interface KafkaProducer<K,V> {
    public void send(ProducerData<K, V> data);

    public boolean isRunning();
}
