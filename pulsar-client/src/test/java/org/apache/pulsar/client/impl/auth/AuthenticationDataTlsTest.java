package org.apache.pulsar.client.impl.auth;

import org.apache.pulsar.client.impl.auth.AuthenticationDataTls;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.security.KeyManagementException;

public class AuthenticationDataTlsTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputNotNullNullOutputIllegalArgumentException()
      throws KeyManagementException {

    // Arrange
    final String certFilePath = "?";
    final String keyFilePath = null;

    // Act, creating object to test constructor
    thrown.expect(IllegalArgumentException.class);
    final AuthenticationDataTls authenticationDataTls =
        new AuthenticationDataTls(certFilePath, keyFilePath);

    // The method is not expected to return due to exception thrown
  }

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputNullNullOutputIllegalArgumentException()
      throws KeyManagementException {

    // Arrange
    final String certFilePath = null;
    final String keyFilePath = null;

    // Act, creating object to test constructor
    thrown.expect(IllegalArgumentException.class);
    final AuthenticationDataTls authenticationDataTls =
        new AuthenticationDataTls(certFilePath, keyFilePath);

    // The method is not expected to return due to exception thrown
  }
}
