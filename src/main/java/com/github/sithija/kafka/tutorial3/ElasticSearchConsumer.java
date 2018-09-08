package com.github.sithija.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ElasticSearchConsumer {

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

        //objective is to write this line to the leastic search cluster
        String jsonString = "{\"foo\":\"bar\"}";

        RestHighLevelClient client = createClient();

        //creating an index request,this takes 3 arguments index,type and id ,id not necesasary here
        IndexRequest request = new IndexRequest("twitter","tweets")
                .source(jsonString, XContentType.JSON);

        //get the id of the response
        IndexResponse indexResponse= client.index(request, RequestOptions.DEFAULT);
        String id = indexResponse.getId();



    }
}
