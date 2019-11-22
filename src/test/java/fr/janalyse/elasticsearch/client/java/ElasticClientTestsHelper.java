package fr.janalyse.elasticsearch.client.java;

import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

abstract public class ElasticClientTestsHelper extends ElasticClientHelper {
  static protected RestHighLevelClient client = null;

  @BeforeAll
  static void clientInit() {
    client = getElasticClient();
  }

  @AfterAll
  static void clientDisposes() {
    try {
      client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
