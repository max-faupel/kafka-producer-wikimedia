package com.example.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WikimediaChangeHandler implements EventHandler {
    KafkaProducer<String, String> kafkaProducer;
    String topic;
    
    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        // nothing here
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info("Data: {}", messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<String,String>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading {}", t);
    }
    
}
