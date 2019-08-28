package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.impl.ProducerStatsDisabled;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class ProducerStatsDisabledTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void getNumAcksReceivedOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getNumAcksReceived());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getNumBytesSentOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getNumBytesSent());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getNumMsgsSentOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getNumMsgsSent());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getNumSendFailedOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getNumSendFailed());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendBytesRateOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendBytesRate(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendLatencyMillis50pctOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendLatencyMillis50pct(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendLatencyMillis75pctOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendLatencyMillis75pct(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendLatencyMillis95pctOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendLatencyMillis95pct(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendLatencyMillis99pctOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendLatencyMillis99pct(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendLatencyMillis999pctOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendLatencyMillis999pct(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendLatencyMillisMaxOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendLatencyMillisMax(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getSendMsgsRateOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0.0, producerStatsDisabled.getSendMsgsRate(), 0.0);
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalAcksReceivedOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getTotalAcksReceived());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalBytesSentOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getTotalBytesSent());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalMsgsSentOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getTotalMsgsSent());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getTotalSendFailedOutputZero() {

    // Arrange
    final ProducerStatsDisabled producerStatsDisabled = new ProducerStatsDisabled();

    // Act and Assert result
    Assert.assertEquals(0L, producerStatsDisabled.getTotalSendFailed());
  }
}
