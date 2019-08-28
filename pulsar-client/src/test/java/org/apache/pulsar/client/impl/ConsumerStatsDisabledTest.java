package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.impl.ConsumerStatsDisabled;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class ConsumerStatsDisabledTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void getNumAcksFailedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getNumAcksFailed());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getNumAcksSentOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getNumAcksSent());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getNumBytesReceivedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getNumBytesReceived());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getNumMsgsReceivedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getNumMsgsReceived());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getNumReceiveFailedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getNumReceiveFailed());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getRateBytesReceivedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, consumerStatsDisabled.getRateBytesReceived(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getRateMsgsReceivedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, consumerStatsDisabled.getRateMsgsReceived(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalAcksFailedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getTotalAcksFailed());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalAcksSentOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getTotalAcksSent());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalBytesReceivedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getTotalBytesReceived());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalMsgsReceivedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getTotalMsgsReceived());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalReceivedFailedOutputZero() {

    // Arrange
    final ConsumerStatsDisabled consumerStatsDisabled = new ConsumerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, consumerStatsDisabled.getTotalReceivedFailed());
  }
}
