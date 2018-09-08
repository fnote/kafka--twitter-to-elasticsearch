package com.github.sithija.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        System.out.println("sithija");

        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

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

        for(int i=0; i<10;i++){

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime record is successfuly sent
                    if (e == null) {
                        //the record was successfully sent
                        logger.info("received new metadata:.\n" + "topic:" + recordMetadata.topic() + "\n" + "partitions:" + recordMetadata.partition()
                                + "\n" + "offsets: " + recordMetadata.offset()
                                + "\n" + "timetsamp:" + recordMetadata.timestamp());

                    } else {
                        logger.error("error", e);

                    }
                }
            });

        }

        producer.flush();

        producer.close();





    }
}
