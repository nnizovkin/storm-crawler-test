package ru.test;

import com.digitalpebble.stormcrawler.Metadata;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ESSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory
            .getLogger(ESUpdaterBolt.class);
    private SpoutOutputCollector collector;
    private Cache<String, String> cache;
    private final int REQUEST_SIZE = 1000;
    private ESConnection esConnection;

    @Override
    public void open(Map stormConf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        esConnection = new ESConnection();
        cache = CacheBuilder.newBuilder().maximumSize(REQUEST_SIZE * 100).build();

    }

    @SneakyThrows
    @Override
    public void nextTuple() {
        String date = Utils.formatDate(System.currentTimeMillis());

        SearchRequest searchRequest = new SearchRequest(esConnection.getIndexName());
        QueryBuilder matchQueryBuilder = QueryBuilders.rangeQuery("seen.next").lte(date);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(matchQueryBuilder);
        sourceBuilder.size(REQUEST_SIZE);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = esConnection.getClient().search(searchRequest, RequestOptions.DEFAULT);

        int counter = 0;
        for (SearchHit hit: searchResponse.getHits().getHits()) {
            String url = ((Map<String, String>)(hit.getSourceAsMap().get("url"))).get("query");
            if(cache.getIfPresent(url) == null) {
                List<Object> fields = Lists.newArrayList(url, new Metadata());
                cache.put(url, "");
                collector.emit(fields, url);
                counter++;
            }
        }

        if(counter == 0) {
            cache.invalidateAll();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }
}