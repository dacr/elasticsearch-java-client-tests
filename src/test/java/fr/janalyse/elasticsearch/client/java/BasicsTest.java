package fr.janalyse.elasticsearch.client.java;

import static org.junit.jupiter.api.Assertions.*;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.UUID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BasicsTest extends ElasticClientTestsHelper {
  @Test
  @DisplayName("elasticsearch client application should be able to get cluster state information")
  void checkCluster() throws IOException {
    ClusterHealthRequest request = new ClusterHealthRequest();
    ClusterHealthResponse healthResponse = client.cluster().health(request, RequestOptions.DEFAULT);
    String clusterName = healthResponse.getClusterName();
    assertTrue(clusterName.length() > 0);
  }

  @Test
  @Order(1)
  @DisplayName("create an index synchronously")
  void createIndex() throws IOException {
    CreateIndexRequest request = new CreateIndexRequest("basics");
    request.settings(
            Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 1)
    );
    CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
    assertTrue(createIndexResponse.isAcknowledged());
  }

  @Test
  @Order(10)
  @DisplayName("Add document synchronously")
  void addDocument() throws IOException {
    String json = "{\"msg\":\"Hello World\"}";
    IndexRequest request =
            new IndexRequest("basics")
                    .source(json, XContentType.JSON);
    IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
    assertTrue(indexResponse.status().getStatus() < 400);
  }

  @Test
  @Order(10)
  @DisplayName("put / update document synchronously")
  void putDocument() throws IOException {
    String json = "{\"msg\":\"Hello World\"}";
    IndexRequest request =
            new IndexRequest("basics")
                    .id("42")
                    .source(json, XContentType.JSON);
    IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
    assertTrue(indexResponse.status().getStatus() < 400);
  }

  @Test
  @Order(11)
  @DisplayName("get document synchronously")
  void getDocument() throws IOException {
    GetRequest request = new GetRequest("basics", "42");
    GetResponse response = client.get(request, RequestOptions.DEFAULT);
    assertTrue( ! response.isSourceEmpty());
  }


  @Test
  @Order(99)
  @DisplayName("delete an index synchronously")
  void deleteIndex() throws IOException {
    DeleteIndexRequest request = new DeleteIndexRequest("basics");
    AcknowledgedResponse deleteResponse = client.indices().delete(request, RequestOptions.DEFAULT);
    assertTrue(deleteResponse.isAcknowledged());
  }

}
