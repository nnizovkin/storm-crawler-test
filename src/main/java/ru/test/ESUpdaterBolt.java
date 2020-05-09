package ru.test;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.URLUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import crawlercommons.domains.PaidLevelDomain;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ESUpdaterBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory
            .getLogger(ESUpdaterBolt.class);
    protected String indexName;
    private RestHighLevelClient client;
    private Cache<String, String> cache;
    private OutputCollector collector;

    public ESUpdaterBolt() {
        super();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        indexName = "pages";
        try {
            final CredentialsProvider credentialsProvider =new BasicCredentialsProvider();
            RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200))
                    .setHttpClientConfigCallback(httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            this.collector = collector;
            client = new RestHighLevelClient(builder);
            cache = CacheBuilder.newBuilder().maximumSize(1000000).build();
        } catch (Exception e1) {
            LOG.error("Can't connect to ElasticSearch", e1);
            throw new RuntimeException(e1);
        }
    }

    @Override
    public void cleanup() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }

    @lombok.SneakyThrows
    @Override
    public void execute(Tuple tuple) {
        String url = valueForURL(tuple);
        Status status = (Status) tuple.getValueByField("status");
        String sha256hex = org.apache.commons.codec.digest.DigestUtils
                .sha256Hex(url);

        if(cache.getIfPresent(sha256hex) != null && status.equals(Status.DISCOVERED)) {
            cache.put(sha256hex, "");
            collector.ack(tuple);
            return;
        }

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");


        String now = Utils.formatDate(System.currentTimeMillis());

        XContentBuilder builder = jsonBuilder().startObject()
                .startObject("url")
                .field("query", url)
                .field("path", getPath(url))
                .field("domain", metadata.getFirstValue("domain"))
                .field("https", url.contains("https"))
                .endObject();

        if(!status.equals(Status.ERROR) && !status.equals(Status.FETCH_ERROR)) {
            builder.field("description", metadata.getFirstValue("parse.description"))
                    .field("title", metadata.getFirstValue("parse.title"))
                    .field("status", metadata.getFirstValue("fetch.statusCode"));
        }
        builder.startObject("seen")
                .field("last", now);

        if(status.equals(Status.DISCOVERED)) {
            builder.field("next", now)
                    .field("first", now);
        } else {
            Instant nextInstant = Instant.now().plusSeconds(60 * 60 * 24 * 30);
            String next = ISODateTimeFormat.basicDate().print(nextInstant.toEpochMilli());
            builder.field("next", next);
            if(status.equals(Status.REDIRECTION)) {
                    builder.field("first", now);
            }
        }

        builder.endObject();
        builder.endObject();
        UpdateRequest request = new UpdateRequest();
        request.docAsUpsert(true);
        request.id(sha256hex);
        request.index(indexName);
        request.doc(builder);

        LOG.debug("Sending to ES buffer {} with ID {}", url, sha256hex);

        client.update(request, RequestOptions.DEFAULT);
        cache.put(sha256hex, "");
        collector.ack(tuple);
    }

    private String getPath(String url) {
        try {
            return new URL(url).getPath();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    protected String valueForURL(Tuple tuple) {

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        String canonicalValue = metadata.getFirstValue("canonical");

        // no value found?
        if (StringUtils.isBlank(canonicalValue)) {
            return url;
        }

        try {
            URL sURL = new URL(url);
            URL canonical = URLUtil.resolveURL(sURL, canonicalValue);

            String sDomain = PaidLevelDomain.getPLD(sURL.getHost());
            String canonicalDomain = PaidLevelDomain
                    .getPLD(canonical.getHost());

            // check that the domain is the same
            if (sDomain.equalsIgnoreCase(canonicalDomain)) {
                return canonical.toExternalForm();
            } else {
                LOG.info(
                        "Canonical URL references a different domain, ignoring in {} ",
                        url);
            }
        } catch (MalformedURLException e) {
            LOG.error("Malformed canonical URL {} was found in {} ",
                    canonicalValue, url);
        }

        return url;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
