package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.impl.schema.ByteBufSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class ByteBufSchemaTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void decodeInputNullOutputNull() {

    // Arrange
    final ByteBufSchema byteBufSchema = new ByteBufSchema();

    // Act and Assert result
    Assert.assertNull(byteBufSchema.decode(null));
  }

  // Test written by Diffblue Cover.
  @Test
  public void encodeInputNullOutputNull() {

    // Arrange
    final ByteBufSchema byteBufSchema = new ByteBufSchema();

    // Act and Assert result
    Assert.assertNull(byteBufSchema.encode(null));
  }
}
