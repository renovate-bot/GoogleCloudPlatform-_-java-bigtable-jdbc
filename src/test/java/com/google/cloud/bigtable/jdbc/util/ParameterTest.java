
package com.google.cloud.bigtable.jdbc.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ParameterTest {

  @Test
  public void testParameter() {
    Parameter parameter = new Parameter("java.lang.String", "test_value");
    assertEquals("java.lang.String", parameter.getTypeLabel());
    assertEquals("test_value", parameter.getValue());
  }
}
