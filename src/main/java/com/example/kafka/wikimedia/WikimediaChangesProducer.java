package com.example.kafka.wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        String serializer = StringSerializer.class.getName();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        properties.keySet().stream().forEach((k) -> {
            log.info("{}: {}", k, properties.get(k));
        });

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            String topic = "wikimedia.recentchange";
            EventHandler handler = new WikimediaChangeHandler(kafkaProducer, topic);
            String url = "https://stream.wikimedia.org/v2/stream/recentchange";
            EventSource.Builder builder = new EventSource.Builder(handler, URI.create(url));
            EventSource eventSource = builder.build();
            eventSource.start();

            TimeUnit.MINUTES.sleep(10);
        }
    }
}
