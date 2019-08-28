package org.apache.pulsar.client.impl.schema;

import static org.mockito.AdditionalMatchers.or;

import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class SchemaUtilsTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void getStringSchemaVersionInput0OutputNotNull() {

    // Arrange
    final byte[] schemaVersionBytes = {};

    // Act and Assert result
    Assert.assertEquals("EMPTY", SchemaUtils.getStringSchemaVersion(schemaVersionBytes));
  }

  // Test written by Diffblue Cover.
  @Test
  public void getStringSchemaVersionInputNullOutputNotNull() {

    // Act and Assert result
    Assert.assertEquals("NULL", SchemaUtils.getStringSchemaVersion(null));
  }

  // Test written by Diffblue Cover.
  @Test
  public void toAvroObjectInputNullOutputNull() {

    // Act and Assert result
    Assert.assertNull(SchemaUtils.toAvroObject(null));
  }
}
