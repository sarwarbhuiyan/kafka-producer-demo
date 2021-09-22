package com.example.sarwarbhuiyan.kafkaproducerdemo.services;

import com.example.sarwarbhuiyan.kafkaproducerdemo.models.Greeting;

public interface Publisher {

    public void publish(Greeting greeting) throws Exception;
}
