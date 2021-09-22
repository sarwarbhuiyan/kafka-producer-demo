package com.example.sarwarbhuiyan.kafkaproducerdemo.controllers;

import com.example.sarwarbhuiyan.kafkaproducerdemo.models.Greeting;
import com.example.sarwarbhuiyan.kafkaproducerdemo.services.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.TimeoutException;


@RestController
public class PublisherResource {

    private static final Logger logger = LoggerFactory.getLogger(PublisherResource.class);

    @Autowired
    Publisher publisher;

    @GetMapping("/")
    public String index() {
        return "Greetings from Spring Boot!";
    }

    /**
     * Post handler taking in a JSON object of shape '{"name": SomeName}'
     * The object is then converted into a POJO, a message attached to it
     * and then published via the Publisher (it happens to be a KafkaPublisher
     * which can then forward onto Kafka
     *
     * @param greeting
     * @return
     * @throws TimeoutException
     */
    @PostMapping("/publish")
    public Greeting publish(@RequestBody Greeting greeting) throws TimeoutException {
        greeting.setMessage("hello "+greeting.getName());
        try {
            publisher.publish(greeting);
            return greeting;
        } catch (Exception e) {
            logger.error("Error occurred sending greeting", e);
            throw new TimeoutException();
        }
    }
}
