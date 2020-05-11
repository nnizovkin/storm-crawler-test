package ru.test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.Getter;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class ESConnection {
    private static final Logger LOG = LoggerFactory
            .getLogger(ESConnection.class);

    private String indexName;
    private RestHighLevelClient client;
    private BulkProcessor bulkProcessor;

    public ESConnection() {
        indexName = "pages";
        try {
            final CredentialsProvider credentialsProvider =new BasicCredentialsProvider();
            RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200))
                    .setHttpClientConfigCallback(httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            client = new RestHighLevelClient(builder);
            bulkProcessor = BulkProcessor
                    .builder((request, bulkListener) -> client.bulkAsync(request,
                            RequestOptions.DEFAULT, bulkListener), new BulkProcessor.Listener() {
                        @Override
                        public void beforeBulk(long executionId, BulkRequest request) {

                        }

                        @Override
                        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

                        }

                        @Override
                        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

                        }
                    })
                    .setFlushInterval(TimeValue.timeValueSeconds(5)).setBulkActions(1000)
                    .setConcurrentRequests(1).build();
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }
    }
}
