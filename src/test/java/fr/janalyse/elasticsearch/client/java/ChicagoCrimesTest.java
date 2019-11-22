package fr.janalyse.elasticsearch.client.java;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChicagoCrimesTest extends ElasticClientTestsHelper {

  @Test
  @DisplayName("elasticsearch client application with chicago crimes index should be able to count the total number of crimes")
  void countCrimes() throws IOException {
    CountRequest request = new CountRequest("crimes");
    CountResponse response = client.count(request, RequestOptions.DEFAULT);
    assertTrue(response.getCount() > 6900000L && response.getCount() < 8000000);
  }

  @Test
  @DisplayName("elasticsearch client application with chicago crimes index should be able to find homicides without arrest")
  void searchCrimesWithoutArrest() throws IOException {
    SearchRequest request = new SearchRequest("crimes");
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder
            .query(QueryBuilders.termQuery("Arrest", false))
            .query(QueryBuilders.termQuery("PrimaryType", "homicide"));
    request.source(searchSourceBuilder);
    SearchResponse response = client.search(request, RequestOptions.DEFAULT);
    long count = response.getHits().getTotalHits().value;
    assertTrue(count > 5000 && count < 6000, "Found " + count);
  }
}
