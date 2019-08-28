package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class MessageIdImplTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputPositivePositivePositiveOutputVoid() {

    // Arrange
    final long ledgerId = 2L;
    final long entryId = 4L;
    final int partitionIndex = 2;

    // Act, creating object to test constructor
    final MessageIdImpl messageIdImpl = new MessageIdImpl(ledgerId, entryId, partitionIndex);

    // Assert side effects
    Assert.assertEquals(4L, messageIdImpl.getEntryId());
  }

  // Test written by Diffblue Cover.
  @Test
  public void equalsInputNotNullOutputFalse() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(2L, 786_432L, 2);
    final MessageIdImpl obj = new MessageIdImpl(524_288L, 262_144L, 3);

    // Act and Assert result
    Assert.assertFalse(messageIdImpl.equals(obj));
  }

  // Test written by Diffblue Cover.
  @Test
  public void equalsInputNotNullOutputFalse2() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(198L, 786_432L, 2);
    final MessageIdImpl obj = new MessageIdImpl(198L, 524_296L, 3);

    // Act and Assert result
    Assert.assertFalse(messageIdImpl.equals(obj));
  }

  // Test written by Diffblue Cover.
  @Test
  public void equalsInputNotNullOutputTrue() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(198L, 786_442L, 3);
    final MessageIdImpl obj = new MessageIdImpl(198L, 786_442L, 3);

    // Act and Assert result
    Assert.assertTrue(messageIdImpl.equals(obj));
  }

  // Test written by Diffblue Cover.
  @Test
  public void getEntryIdOutputPositive() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(3L, 2L, 2);

    // Act and Assert result
    Assert.assertEquals(2L, messageIdImpl.getEntryId());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getLedgerIdOutputPositive() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(2L, 344_644L, 2);

    // Act and Assert result
    Assert.assertEquals(2L, messageIdImpl.getLedgerId());
  }

  // Test written by Diffblue Cover.
  @Test
  public void getPartitionIndexOutputPositive() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(2L, 3L, 2);

    // Act and Assert result
    Assert.assertEquals(2, messageIdImpl.getPartitionIndex());
  }

  // Test written by Diffblue Cover.
  @Test
  public void hashCodeOutputPositive() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(942_707L, 562_517L, 2);

    // Act and Assert result
    Assert.assertEquals(569_802_756, messageIdImpl.hashCode());
  }

  // Test written by Diffblue Cover.
  @Test
  public void toStringOutputNotNull() {

    // Arrange
    final MessageIdImpl messageIdImpl = new MessageIdImpl(2L, 524_297L, 2);

    // Act and Assert result
    Assert.assertEquals("2:524297:2", messageIdImpl.toString());
  }
}
