package nl.oliveira;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        final var client = createOpenSearchClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        try (client; consumer) {
            final boolean hasIndex = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!hasIndex) {
                CreateIndexRequest request = new CreateIndexRequest("wikimedia");
                client.indices().create(request, RequestOptions.DEFAULT);
                log.info("Wikimedia has been created");
            } else {
                log.info("Wikimedia already created");
            }

            consumer.subscribe(Collections.singletonList("wikimedia.recent-change"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                log.info("Received {} records", records.count());

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    final var id = extractId(record.value());

                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    bulkRequest.add(indexRequest);

                    IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

                    log.info("Key: {}, value: {} ", record.key(), record.value());
                    log.info("Partition: {}, Offset: {}",record.partition(), record.offset());
                    log.info(response.getId());
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {}", bulkResponse.getItems().length);
                }
            }
        }
    }

    private static String extractId(String record) {
        //id = record.topic() + record.partition() + record.offset()
        return JsonParser.parseString(record)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, "opensearch-consumer-group");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient() {
        final var source = "http://localhost:9200";
        final var uri = URI.create(source);
        final var userInfo = uri.getUserInfo();

        RestHighLevelClient restHighLevelClient;

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme())));
        } else {
            final String [] auth = userInfo.split(":");
            final CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(provider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                            )
            );
        }

        return restHighLevelClient;
    }
}
