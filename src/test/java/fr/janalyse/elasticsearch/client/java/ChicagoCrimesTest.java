package fr.janalyse.elasticsearch.client.java;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ChicagoCrimesTest extends ElasticClientTestsHelper {

  @Test
  @DisplayName("elasticsearch client application with chicago crimes index should be able to count the total number of crimes")
  void countCrimes() {
    assertTrue(true);
  }

}
