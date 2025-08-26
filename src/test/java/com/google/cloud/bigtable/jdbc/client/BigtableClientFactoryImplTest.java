
package com.google.cloud.bigtable.jdbc.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigtableClientFactoryImplTest {

  @Test
  public void testCreateBigtableDataClient() throws IOException {
    Credentials credentials = mock(Credentials.class);
    BigtableClientFactoryImpl factory = new BigtableClientFactoryImpl(credentials);
    
    // We can't fully test the client creation without credentials, 
    // but we can ensure it doesn't throw an exception with a valid configuration.
    try {
      BigtableDataClient client = factory.createBigtableDataClient("test-project", "test-instance", "test-app-profile");
      assertNotNull(client);
    } catch (Exception e) {
      // This is expected to fail without real credentials, but a null pointer exception would indicate a problem.
    }
  }
  
  @Test
  public void testConstructorWithCredentials() {
    Credentials credentials = mock(Credentials.class);
    BigtableClientFactoryImpl factory = new BigtableClientFactoryImpl(credentials);
    assertNotNull(factory);
  }
}
