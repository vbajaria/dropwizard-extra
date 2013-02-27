package com.datasift.dropwizard.kafka.producer;

import com.yammer.dropwizard.lifecycle.Managed;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AsyncProducer<K,V> implements Managed, KafkaProducer<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncProducer.class);

    private Producer<K,V> producer;

    public AsyncProducer(Producer<K,V> producer) {
        this.producer = producer;
    }

    @Override
    public void start() throws Exception {
        LOG.info("starting AsyncProducer with configuration : " + producer.toString());
    }

    @Override
    public void stop() throws Exception {
        this.producer.close();
    }

    @Override
    public void send(ProducerData<K, V> data) {
        this.producer.send(data);
    }

    public void send(List<ProducerData<K, V>> dataList) {
        this.producer.send(dataList);
    }

    @Override
    public boolean isRunning() {
        //TODO: figure out a healthcheck
        return true;
    }
}
