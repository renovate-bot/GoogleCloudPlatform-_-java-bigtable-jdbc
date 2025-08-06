package com.google.cloud.bigtable.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.Test;

public class BigtableConnectionTest {
  private final String baseURL = "jdbc:bigtable:/projects/test-project/instances/test-instance";
  Properties properties = new Properties();

  @Test
  public void testValidClientCreation() throws SQLException {
    BigtableConnection bigtableConnection = new BigtableConnection(baseURL, properties);
    assertNotNull(bigtableConnection);
  }

  @Test
  public void testValidClientCreationWithQueryParams() throws SQLException {
    String queryParamsURL = baseURL + "?app_profile_id=dev&universe_domain=default";
    BigtableConnection bigtableConnection = new BigtableConnection(queryParamsURL, properties);
    assertNotNull(bigtableConnection);
  }
}
