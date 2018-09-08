package com.github.sithija.kafka.tutorial1;

import com.sun.org.omg.SendingContext.CodeBasePackage.ValueDescSeqHelper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        final Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties= new Properties();
        String group_id ="my-fourth-application";
        String topic = "first_topic";
        //refer consumer config table on kafka docs

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //takes the bytes from kafka and produces a string from it deserializer
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);


        //subscribe cisnumer to topic
        consumer.subscribe(Arrays.asList("first_topic"));

        //poll for  new data
        while(true){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record:records){
                logger.info("key:"+ record.key());
                logger.info("partitions:"+ record.partition());
                logger.info("value:"+record.value());
                logger.info("topic:"+record.topic());
            }

        }







    }
}
