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
package org.apache.pulsar.functions.source;

import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.Test;
import sun.net.www.content.text.Generic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertSame;
import static org.testng.AssertJUnit.*;

@Slf4j
public class TopicSchemaTest {

    private static final String TOPIC_NAME = "persistent://public/default/topic";

    @Test
    public void testGenericRecordForConsumer() throws Exception {
        PulsarClient pulsarClient = mock(PulsarClient.class);
        TopicSchema topicSchema = new TopicSchema(pulsarClient);
        ConsumerConfig consumerConfig = new ConsumerConfig();
        assertNull(consumerConfig.getSchemaType());
        Schema<?> schema = topicSchema.getSchema(TOPIC_NAME, GenericRecord.class, consumerConfig, true);
        assertEquals(schema.getClass(), AutoConsumeSchema.class);
    }

    @Test
    public void testGenericRecordForProducer() throws Exception {
        PulsarClient pulsarClient = mock(PulsarClient.class);
        TopicSchema topicSchema = new TopicSchema(pulsarClient);
        ConsumerConfig consumerConfig = new ConsumerConfig();
        assertNull(consumerConfig.getSchemaType());
        // this is what happens in PulsarSink when is attached to a GenericRecord datatype
        Schema<?> schema = topicSchema.getSchema(TOPIC_NAME, GenericRecord.class, consumerConfig, false);
        assertEquals(schema.getClass(), AutoProduceBytesSchema.class);
    }


}
