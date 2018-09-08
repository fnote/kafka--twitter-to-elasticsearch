package com.github.sithija.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("sithija");

        final Logger logger= LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

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


        for(int i=0; i<10;i++){
        //create a producer record
            String topic = "first_topic ";
            String value = "hello world"+ Integer.toString(i) ;
            String key = "id_"+ Integer.toString(i);

        ProducerRecord<String,String>  record= new ProducerRecord<String, String>(topic,key,value);
        //send data
        logger.info("key:" + key);


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

            }).get();

        }

        producer.flush();

        producer.close();





    }
}
