package org.apache.pulsar.client.impl.auth;

import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.lang.reflect.Method;

public class AuthenticationTlsTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputNotNullNotNullOutputVoid() {

    // Arrange
    final String certFilePath = "/";
    final String keyFilePath = "/";

    // Act, creating object to test constructor
    final AuthenticationTls authenticationTls = new AuthenticationTls(certFilePath, keyFilePath);

    // Assert side effects
    Assert.assertEquals("/", authenticationTls.getKeyFilePath());
    Assert.assertEquals("/", authenticationTls.getCertFilePath());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getAuthMethodNameOutputNotNull() {

    // Arrange
    final AuthenticationTls authenticationTls = new AuthenticationTls();

    // Act and Assert result
    Assert.assertEquals("tls", authenticationTls.getAuthMethodName());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getCertFilePathOutputNull() {

    // Arrange
    final AuthenticationTls authenticationTls = new AuthenticationTls();

    // Act and Assert result
    Assert.assertNull(authenticationTls.getCertFilePath());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getKeyFilePathOutputNull() {

    // Arrange
    final AuthenticationTls authenticationTls = new AuthenticationTls();

    // Act and Assert result
    Assert.assertNull(authenticationTls.getKeyFilePath());
  }
}
