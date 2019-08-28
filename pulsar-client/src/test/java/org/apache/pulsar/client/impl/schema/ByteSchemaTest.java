package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.lang.reflect.Array;

public class ByteSchemaTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void decodeInput1OutputZero() {

    // Arrange
    final ByteSchema byteSchema = new ByteSchema();
    final byte[] bytes = {(byte)0};

    // Act and Assert result
    Assert.assertEquals(new Byte((byte)0), byteSchema.decode(bytes));
  }

  // Test written by Diffblue Cover.
  @Test
  public void decodeInputNullOutputNull() {

    // Arrange
    final ByteSchema byteSchema = new ByteSchema();

    // Act and Assert result
    Assert.assertNull(byteSchema.decode(null));
  }

  // Test written by Diffblue Cover.
  @Test
  public void encodeInputNegativeOutput1() {

    // Arrange
    final ByteSchema byteSchema = new ByteSchema();

    // Act
    final byte[] actual = byteSchema.encode((byte)-63);

    // Assert result
    Assert.assertArrayEquals(new byte[] {(byte)-63}, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void encodeInputNullOutputNull() {

    // Arrange
    final ByteSchema byteSchema = new ByteSchema();

    // Act and Assert result
    Assert.assertNull(byteSchema.encode(null));
  }
}
