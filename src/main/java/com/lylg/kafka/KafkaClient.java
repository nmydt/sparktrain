package com.lylg.kafka;

public class KafkaClient {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();
    }
}
