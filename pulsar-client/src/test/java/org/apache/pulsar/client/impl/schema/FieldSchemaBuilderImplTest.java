package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.impl.schema.FieldSchemaBuilderImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class FieldSchemaBuilderImplTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void buildOutputNullPointerException() {

    // Arrange
    final FieldSchemaBuilderImpl fieldSchemaBuilderImpl = new FieldSchemaBuilderImpl("/");

    // Act
    thrown.expect(NullPointerException.class);
    fieldSchemaBuilderImpl.build();

    // The method is not expected to return due to exception thrown
  }
}
