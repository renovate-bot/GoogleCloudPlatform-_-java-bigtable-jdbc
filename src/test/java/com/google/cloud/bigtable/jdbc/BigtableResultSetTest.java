package com.google.cloud.bigtable.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.data.v2.models.sql.ResultSet;
import com.google.cloud.bigtable.data.v2.models.sql.ResultSetMetadata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class BigtableResultSetTest {
  private ResultSet mockedBigtableResultSet;
  private ResultSetMetadata mockedMetadata;
  private BigtableResultSet resultSet;

  @Before
  public void setUp() {
    mockedBigtableResultSet = mock(ResultSet.class);
    mockedMetadata = mock(ResultSetMetadata.class);

    when(mockedBigtableResultSet.getMetadata()).thenReturn(mockedMetadata);

    List<Map<String, Object>> rows = new ArrayList<>();

    resultSet = new BigtableResultSet(mockedBigtableResultSet, rows);
  }

  @Test
  public void testNextAndGetString() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedMetadata.getColumnIndex("col2")).thenReturn(1);

    when(mockedBigtableResultSet.getString(0)).thenReturn("value1").thenReturn("value2");

    when(mockedBigtableResultSet.getString(1)).thenReturn("123").thenReturn("456");

    assertTrue(resultSet.next());
    assertEquals("value1", resultSet.getString("col1"));
    assertEquals("123", resultSet.getString("col2"));

    assertTrue(resultSet.next());
    assertEquals("value2", resultSet.getString("col1"));
    assertEquals("456", resultSet.getString("col2"));

    assertFalse(resultSet.next());
  }

  @Test
  public void testGetStringReturnsNullIfValueIsNull() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("col1")).thenReturn(0);
    when(mockedBigtableResultSet.getString(0)).thenReturn(null);

    assertTrue(resultSet.next());
    assertNull(resultSet.getString("col1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetStringThrowsExceptionIfColumnInvalid() throws SQLException {
    when(mockedBigtableResultSet.next()).thenReturn(true);
    when(mockedMetadata.getColumnIndex("invalid"))
        .thenThrow(new IllegalArgumentException("Invalid column"));

    assertTrue(resultSet.next());
    resultSet.getString("invalid");
  }
}
