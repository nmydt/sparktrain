package com.lylg.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducer extends Thread {
    private final String topic;
    private final Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("metadata.broker.list", KafkaProperties.BROKER_LIST);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<Integer, String>(config);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String message = "message " + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("sent: " + message);
            messageNo++;

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
