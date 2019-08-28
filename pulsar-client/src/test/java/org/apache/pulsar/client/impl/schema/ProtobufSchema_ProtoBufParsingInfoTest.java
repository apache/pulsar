package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.impl.schema.ProtobufSchema.ProtoBufParsingInfo;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.util.HashMap;

public class ProtobufSchema_ProtoBufParsingInfoTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.

  @Test
  public void constructorInputNegativeNotNullNotNullNotNull1OutputVoid() {

    // Arrange
    final int number = -999_998;
    final String name = "foo";
    final String type = "foo";
    final String label = "foo";
    final HashMap<String, Object> definition = new HashMap<String, Object>();
    definition.put(null, null);

    // Act, creating object to test constructor
    final ProtoBufParsingInfo protobufSchemaProtoBufParsingInfo =
        new ProtoBufParsingInfo(number, name, type, label, definition);

    // Assert side effects
    Assert.assertEquals("foo", protobufSchemaProtoBufParsingInfo.getType());
    Assert.assertEquals("foo", protobufSchemaProtoBufParsingInfo.getLabel());
    final HashMap<String, Object> hashMap = new HashMap<String, Object>();
    hashMap.put(null, null);
    Assert.assertEquals(hashMap, protobufSchemaProtoBufParsingInfo.getDefinition());
    Assert.assertEquals("foo", protobufSchemaProtoBufParsingInfo.getName());
  }
}
