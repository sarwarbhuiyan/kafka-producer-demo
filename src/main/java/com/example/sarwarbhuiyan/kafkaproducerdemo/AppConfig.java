package com.example.sarwarbhuiyan.kafkaproducerdemo;

import com.example.sarwarbhuiyan.kafkaproducerdemo.services.Publisher;
import com.example.sarwarbhuiyan.kafkaproducerdemo.services.impl.KafkaPublisher;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "kafka.publisher")
public class AppConfig {

    private Map<String,String> properties;

    /**
     * Needed to inject map of properties from application.yml
     *
     * @param props
     */
    public void setProperties(Map<String, String> props) {
        this.properties = props;
    }

    @Bean
    public Publisher createKafkaPublisher() {
        Publisher publisher = new KafkaPublisher(properties);
        return publisher;
    }
}
