package com.github.sithija.kafka.tutorial3;

import com.github.sithija.kafka.tutorial1.ConsumerDemoGroups;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    final Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static RestHighLevelClient createClient(){

        //creating an elastic search client
        //https://:@

        String hostname="kafka-course-1081256156.eu-west-1.bonsaisearch.net";
        String username ="x33iyt0tzd";
        String password="i7wxejam47";

        CredentialsProvider credentialsProvider= new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                });


        RestHighLevelClient client =new RestHighLevelClient(builder);

        return client;
    }

    public static void main(String[] args) throws IOException {

        final Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        //objective is to write this line to the leastic search cluster
        String jsonString = "{\"foo\":\"bar\"}";

        RestHighLevelClient client = createClient();


        KafkaConsumer<String,String> consumer= createConsumer("twitter_tweets");


        while(true){


            BulkRequest bulkRequest=new BulkRequest();

            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));


            Integer recordCount= records.count();

            for(ConsumerRecord record:records){



                //kafka generic id
//                String id = record.topic()+ "_" + record.partition() +"_" + record.offset();
              //where we insert data to elastic search


                try{

                    //twitter feed specific id handling idempotence data delivery semantics
                    String id = extractIdFromTweet(record.value());

                    //creating an index request,this takes 3 arguments index,type and id ,id not necesasary here
                    IndexRequest request = new IndexRequest("twitter","tweets",id)
                            .source(record.value(), XContentType.JSON);

                    bulkRequest.add(request); //here we add to pur bulk request

                }catch (NullPointerException e){
                    logger.warn("skipping bad data"+ record.value());
                }



                //get the id of the response,this is not necessary just for testing get id use id to see its in cluster
//                IndexResponse indexResponse= client.index(request, RequestOptions.DEFAULT);
////                String id = indexResponse.getId();
//                logger.info(indexResponse.getId());
//
//
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
            }

            if (recordCount>0) {
                //only if there are records only ...10 records added at once
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("commiting offsert ");
                consumer.commitSync();
                logger.info("offsets have been commited ");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }


//        client.close();

    }

    public static KafkaConsumer<String,String> createConsumer(String topic ){

//

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties= new Properties();
        String group_id ="kafka-demo-elasticsearch";
//        String topic = "twitter_tweets";
        //refer consumer config table on kafka docs

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //takes the bytes from kafka and produces a string from it deserializer
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); //disable auto commit of records
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10"); //only pulls 10 recordss


        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }

    private static JsonParser jsonParser =new JsonParser();

    private static String extractIdFromTweet(String tweetJson){
        //can use gson library

        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }


}
