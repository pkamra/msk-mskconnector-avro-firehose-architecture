package com.example.kafkaproducer.consumer;

import com.example.kafkaproducer.topicproducer.TopicProducer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TopicConsumer {

    @Value("${topic.name.consumer}")
    private String topicName;

    Logger logger = LoggerFactory.getLogger(TopicConsumer.class);


    @KafkaListener(topics = "${topic.name.consumer}", groupId = "local-consumer")
    public void consume(ConsumerRecord<String, GenericRecord> payload){logger.info("Topic: {}", topicName);
//        logger.info("key: {}", payload.key());
//        logger.info("Headers: {}", payload.headers());
//        logger.info("Partion: {}", payload.partition());
//        logger.info("User: {}", payload.value());
        final GenericRecord value = payload.value();
        System.out.println("Received message: value = " + value);

    }
}
