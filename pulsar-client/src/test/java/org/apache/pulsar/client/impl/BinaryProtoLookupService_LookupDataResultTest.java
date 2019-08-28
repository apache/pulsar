package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class BinaryProtoLookupService_LookupDataResultTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputPositiveOutputVoid() {

    // Arrange
    final int partitions = 4;

    // Act, creating object to test constructor
    final LookupDataResult binaryProtoLookupServiceLookupDataResult =
        new LookupDataResult(partitions);

    // Assert side effects
    Assert.assertEquals(4, binaryProtoLookupServiceLookupDataResult.partitions);
  }
}
