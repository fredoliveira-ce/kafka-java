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

        final var client = createOpenSearchClient();

        try (client) {
            final boolean hasIndex = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

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
