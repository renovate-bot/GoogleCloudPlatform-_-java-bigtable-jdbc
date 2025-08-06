package com.google.cloud.bigtable.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BigtableDriverTest {
  private BigtableDriver driver;

  @Before
  public void setUp() {
    driver = new BigtableDriver();
  }

  @Test
  public void testAcceptsURL() throws SQLException {
    String validUrl = "jdbc:bigtable:/projects/test-project/instances/test-instance";
    assertTrue(driver.acceptsURL(validUrl));

    String invalidUrl = "jdbc:mysql://localhost:3306/db";
    assertFalse(driver.acceptsURL(invalidUrl));

    String emptyPrefix = "";
    assertFalse(driver.acceptsURL(emptyPrefix));
  }

  @Test
  public void testConnectWithValidURL() throws SQLException {
    Driver mockDriver = Mockito.mock(BigtableDriver.class);
    Connection mockConnection = Mockito.mock(Connection.class);

    Mockito.when(mockDriver.connect(Mockito.anyString(), Mockito.any())).thenReturn(mockConnection);

    Connection connection =
        mockDriver.connect(
            "jdbc:bigtable:/projects/test-project/instances/test-instance", new Properties());

    assertNotNull(connection);
    assertFalse(connection.isClosed());
  }

  @Test
  public void testConnectWithInvalidURL() {
    String invalidUrl = "jdbc:invalid-driver:/projects/demo/instances/test";
    Properties props = new Properties();
    try {
      driver.connect(invalidUrl, props);
      fail("Expected SQLException to be thrown due to invalid URL");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Does not start with jdbc:bigtable:/"));
    }
  }

  @Test
  public void testGetMajorAndMinorVersion() {
    assertEquals(1, driver.getMajorVersion());
    assertEquals(9, driver.getMinorVersion());
  }

  @Test
  public void testJdbcCompliantReturnsFalse() {
    assertFalse(driver.jdbcCompliant());
  }
}
