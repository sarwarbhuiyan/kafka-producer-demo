package com.example.sarwarbhuiyan.kafkaproducerdemo.services.impl;

import com.example.sarwarbhuiyan.kafkaproducerdemo.models.Greeting;
import com.example.sarwarbhuiyan.kafkaproducerdemo.services.Publisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KafkaPublisher implements Publisher {

    private Producer<String, Greeting> producer;
    private final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);
    private String topic;

    public KafkaPublisher(Map<String, String> config) {
        Map<String, Object> combinedConfig = new HashMap<>();

        // put some basic properties in first
        combinedConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        combinedConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // merge in the properties from application.yml. there could be overrides too
        combinedConfig.putAll(config);
        this.topic = config.get("greetings-topic");
        producer = new KafkaProducer<String, Greeting>(combinedConfig);
    }

    @Override
    public void publish(Greeting greeting) throws Exception {
        try {
            ProducerRecord<String, Greeting> record = new ProducerRecord<>(this.topic, greeting.getName(), greeting);

            producer.send(record, (recordMetadata, e) -> {
                if(e != null)
                    logger.error("Error publishing greeting", e);
                else
                    logger.info("Acknowledgement received for {}", recordMetadata);
            }).get(1000L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Unhandled exception occurred while trying to send data to Kafka", e);
        }


    }
}
