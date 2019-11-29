package fr.janalyse.elasticsearch.client.java.helpers;

import org.apache.http.HttpHost;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

public class ElasticClientDynamicProvisionedTestsHelper {

  static protected RestHighLevelClient client = null;
  static private ElasticsearchClusterRunner runner = new ElasticsearchClusterRunner();

  @BeforeAll
  static void startupElasticsearch() {

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
    runner.ensureYellow();

    // get client connection
    client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("localhost", 9201, "http")
            ));
  }

  @AfterAll
  static void shutdownElasticsearch() throws IOException {
    client.close();
    runner.close();
    runner.clean();
  }


}
