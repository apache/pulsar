package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.impl.ConsumerId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class ConsumerIdTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void getSubscriptionOutputNotNull() {

    // Arrange
    final ConsumerId consumerId = new ConsumerId("/", "/");

    // Act and Assert result
    Assert.assertEquals("/", consumerId.getSubscription());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTopicOutputNotNull() {

    // Arrange
    final ConsumerId consumerId = new ConsumerId("/", "/");

    // Act and Assert result
    Assert.assertEquals("/", consumerId.getTopic());
  }
}
