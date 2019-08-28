package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class AutoConsumeSchemaTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void supportSchemaVersioningOutputTrue() {

    // Arrange
    final AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();

    // Act and Assert result
    Assert.assertTrue(autoConsumeSchema.supportSchemaVersioning());
  }
}
