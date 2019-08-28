package org.apache.pulsar.client.impl.auth;

import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.lang.reflect.Method;

public class AuthenticationBasicTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void getAuthMethodNameOutputNotNull() {

    // Arrange
    final AuthenticationBasic authenticationBasic = new AuthenticationBasic();

    // Act and Assert result
    Assert.assertEquals("basic", authenticationBasic.getAuthMethodName());
  }
}
