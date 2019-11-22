package fr.janalyse.elasticsearch.client.java;

import static org.junit.jupiter.api.Assertions.*;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.junit.jupiter.api.*;

import java.io.IOException;

public class BasicsTest extends ElasticClientTestsHelper {
  @Test
  @DisplayName("elasticsearch client application should be able to get cluster state information")
  void checkCluster() throws IOException {
    ClusterHealthRequest request = new ClusterHealthRequest();
    ClusterHealthResponse healthResponse = client.cluster().health(request, RequestOptions.DEFAULT);
    String clusterName = healthResponse.getClusterName();
    assertTrue(clusterName.length() > 0);
  }

}
