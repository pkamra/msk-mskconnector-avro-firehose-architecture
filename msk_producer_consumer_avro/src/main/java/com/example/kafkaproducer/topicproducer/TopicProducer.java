package com.example.kafkaproducer.topicproducer;


import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import java.io.File;
import java.io.InputStream;

@Service
@RestController
@RequestMapping("/kafkaproduce")
public class TopicProducer {

    Logger logger = LoggerFactory.getLogger(TopicProducer.class);

    @Value("${topic.name.producer}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    @GetMapping(value = "/send")
    public void send(String message){
        try {
            logger.info("Payload to be sent: {}", message);
            InputStream inputStream = new ClassPathResource("Customer.avsc").getInputStream();
            Schema schema_customer = new Parser().parse(inputStream);
            GenericRecord customer = new GenericData.Record(schema_customer);
            customer.put("first_name", "Ada");
            customer.put("last_name", "Lovelace");
            kafkaTemplate.send(topicName, customer);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

}