package org.codecraftlabs.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public RestHighLevelClient createClient() {
        String hostname = "";
        String userName = "";
        String password = "";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    public KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }


    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer consumer = new ElasticSearchConsumer();
        RestHighLevelClient client =  consumer.createClient();

        KafkaConsumer<String, String> kafkaConsumer = consumer.createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String jsonString = record.value();
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = response.getId();
                logger.info("Response id: " + id);
            }
        }

        //client.close();
    }
}
