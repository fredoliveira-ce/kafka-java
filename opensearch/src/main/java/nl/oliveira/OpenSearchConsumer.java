package nl.oliveira;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;


public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        RestHighLevelClient client = createOpenSearchClient();

        try (client) {
            boolean hasIndex = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!hasIndex) {
                CreateIndexRequest request = new CreateIndexRequest("wikimedia");
                client.indices().create(request, RequestOptions.DEFAULT);
                log.info("Wikimedia has been created");
            } else {
                log.info("Wikimedia already created");
            }

        }
    }

    private static RestHighLevelClient createOpenSearchClient() {
        String source = "http://localhost:9200";

        RestHighLevelClient restHighLevelClient;
        URI uri = URI.create(source);
        String userInfo = uri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme())));
        } else {
            String [] auth = userInfo.split(":");
            CredentialsProvider provider = new BasicCredentialsProvider();
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
