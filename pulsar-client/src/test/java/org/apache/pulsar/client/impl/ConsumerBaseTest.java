package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.impl.ConsumerBase.ConsumerType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class ConsumerBaseTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.

  @Test
  public void valueOfInputNullOutputNullPointerException() {

    // Arrange
    final String name = null;

    // Act
    thrown.expect(NullPointerException.class);
    ConsumerType.valueOf(name);

    // The method is not expected to return due to exception thrown
  }
}
