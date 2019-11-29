package fr.janalyse.elasticsearch.client.java;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.http.HttpHost;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.UUID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BasicsTest {

  static private RestHighLevelClient client = null;

  @BeforeAll
  static void startupElasticsearch() {

    ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner();
    // create ES nodes
    runner.onBuild(new ElasticsearchClusterRunner.Builder() {
      @Override
      public void build(final int number, final Settings.Builder settingsBuilder) {
        // put elasticsearch settings
        //settingsBuilder.put("index.number_of_replicas", 0);
      }
    }).build(
            ElasticsearchClusterRunner
                    .newConfigs()
                    .basePath("test-elastic-data")
                    .numOfNode(1)
    );

    /*
    runner.build( {
      ElasticsearchClusterRunner.newConfigs()
              .numOfNode(1)
              .basePath("elastic-data")
              .clusterName("my-name")
    }
    runner.ensureYellow()
     */
    // get client connection
    client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9201, "http")
            ));
  }

  @AfterAll
  static void shutdownElasticsearch() throws IOException {
    client.close();
  }


  @Test
  @DisplayName("elasticsearch client application should be able to get cluster state information")
  void checkCluster() throws IOException {
    ClusterHealthRequest request = new ClusterHealthRequest();
    ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
    assertTrue(response.getNumberOfNodes() > 0);
    assertTrue(response.getClusterName().length() > 0);
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
    CreateIndexResponse responses = client.indices().create(request, RequestOptions.DEFAULT);
    assertTrue(responses.isAcknowledged());
  }

  @Test
  @Order(10)
  @DisplayName("Add document synchronously")
  void addDocument() throws IOException {
    String json = "{\"msg\":\"Hello World\"}";
    IndexRequest request =
            new IndexRequest("basics")
                    .source(json, XContentType.JSON);
    IndexResponse response = client.index(request, RequestOptions.DEFAULT);
    assertTrue(response.status().getStatus() < 400);
  }

  @Test
  @Order(10)
  @DisplayName("add document with ID synchronously")
  void addDocumentWithId() throws IOException {
    String json = "{\"msg\":\"Hello World\"}";
    IndexRequest request =
            new IndexRequest("basics")
                    .id("42")
                    .source(json, XContentType.JSON);
    IndexResponse response = client.index(request, RequestOptions.DEFAULT);
    assertTrue(response.status().getStatus() < 400);
  }

  @Test
  @Order(20)
  @DisplayName("update document  synchronously")
  void updateDocument() throws IOException {
    String json = "{\"msg\":\"Hello World !!\"}";
    UpdateRequest request =
            new UpdateRequest("basics", "42")
                    .doc(json, XContentType.JSON);
    UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
    assertTrue(response.status().getStatus() < 400);
  }


  @Test
  @Order(30)
  @DisplayName("get document synchronously")
  void getDocument() throws IOException {
    GetRequest request = new GetRequest("basics", "42");
    GetResponse response = client.get(request, RequestOptions.DEFAULT);
    assertTrue(!response.isSourceEmpty());
    assertTrue(response.getVersion() == 2);
  }

  @Test
  @Order(30)
  @DisplayName("upsert document synchronously, insert if it doesn't exist")
  void upsertAsInsertDocument() throws IOException {
    //info("If the document doesn't exist it is inserted, if it exists then it is updated");
    String json = "{\"msg\":\"Bonjour tout le monde !\"}";
    UpdateRequest request =
            new UpdateRequest("basics", "4242")
                    .doc(json, XContentType.JSON)
                    .docAsUpsert(true);
    UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
    assertTrue(response.status().getStatus() < 400);
  }

  @Test
  @Order(35)
  @DisplayName("upsert document synchronously, update if it exists")
  void upsertAsUpdateDocument() throws IOException {
    //info("If the document doesn't exist it is inserted, if it exists then it is updated");
    String json = "{\"msg\":\"Bonjour tout le monde !\"}";
    UpdateRequest request =
            new UpdateRequest("basics", "4242")
                    .doc(json, XContentType.JSON)
                    .docAsUpsert(true);
    UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
    assertTrue(response.status().getStatus() < 400);
  }

  @Test
  @Order(40)
  @DisplayName("refresh synchronously in order to get the right count")
  void refresh() throws IOException {
    //info("Take care of performance, this is not something you should do");
    RefreshRequest request = new RefreshRequest("basics");
    RefreshResponse response = client.indices().refresh(request, RequestOptions.DEFAULT);
    assertTrue(response.getStatus().getStatus() < 400);
  }

  @Test
  @Order(50)
  @DisplayName("count document synchronously")
  void countDocuments() throws IOException {
    CountRequest request = new CountRequest("basics");
    CountResponse response = client.count(request, RequestOptions.DEFAULT);
    assertTrue(response.getCount() == 3L, "current size is " + response.getCount());
  }


  @Test
  @Order(99)
  @DisplayName("delete an index synchronously")
  void deleteIndex() throws IOException {
    DeleteIndexRequest request = new DeleteIndexRequest("basics");
    AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
    assertTrue(response.isAcknowledged());
  }

}
