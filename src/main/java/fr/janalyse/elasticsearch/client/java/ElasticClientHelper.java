package fr.janalyse.elasticsearch.client.java;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

abstract public class ElasticClientHelper {

  static public RestHighLevelClient getElasticClient() {
    return getElasticClient(9201);
  }
  static public RestHighLevelClient getElasticClient(int elasticPort) {
    RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", elasticPort, "http")
            )
    );
    return client;
  }
}
