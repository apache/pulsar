package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.impl.schema.ByteBufferSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class ByteBufferSchemaTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void decodeInputNullOutputNull() {

    // Arrange
    final ByteBufferSchema byteBufferSchema = new ByteBufferSchema();

    // Act and Assert result
    Assert.assertNull(byteBufferSchema.decode(null));
  }

  // Test written by Diffblue Cover.
  @Test
  public void encodeInputNullOutputNull() {

    // Arrange
    final ByteBufferSchema byteBufferSchema = new ByteBufferSchema();

    // Act and Assert result
    Assert.assertNull(byteBufferSchema.encode(null));
  }
}
