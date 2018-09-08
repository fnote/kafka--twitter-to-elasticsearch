package com.github.sithija.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        System.out.println("sithija");

        //create producer properties
        String boostrapServers = "127.0.0.1:9092";

        Properties properties= new Properties();

//        properties.setProperty("bootstrap.servers",boostrapServers);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create the producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String,String>  record= new ProducerRecord<String, String>("first_topic","hello world");
        //send data

        producer.send(record);

        producer.flush();

        producer.close();





    }
}
