/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.schema.generic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for {@link MultiVersionSchemaInfoProvider}.
 */
public class MultiVersionSchemaInfoProviderTest {

    private MultiVersionSchemaInfoProvider schemaProvider;

    @BeforeMethod
    public void setup() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.getLookup()).thenReturn(mock(LookupService.class));
        schemaProvider = new MultiVersionSchemaInfoProvider(
                TopicName.get("persistent://public/default/my-topic"), client);
    }

    @Test
    public void testGetSchema() throws Exception {
        CompletableFuture<Optional<SchemaInfo>> completableFuture = new CompletableFuture<>();
        SchemaInfo schemaInfo = AvroSchema.of(SchemaDefinition.<SchemaTestUtils>builder().withPojo(SchemaTestUtils.class).build()).getSchemaInfo();
        completableFuture.complete(Optional.of(schemaInfo));
        when(schemaProvider.getPulsarClient().getLookup()
                .getSchema(
                        any(TopicName.class),
                        any(byte[].class)))
                .thenReturn(completableFuture);
        SchemaInfo schemaInfoByVersion = schemaProvider.getSchemaByVersion(new byte[0]).get();
        assertEquals(schemaInfoByVersion, schemaInfo);
    }
}
