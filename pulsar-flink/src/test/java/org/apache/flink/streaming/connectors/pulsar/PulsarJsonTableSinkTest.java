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
package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;

/**
 * Unit test of {@link PulsarJsonTableSink}.
 */
public class PulsarJsonTableSinkTest {

    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC_NAME = "test_topic";
    private static final String ROUTING_KEY = "key";
    private final String[] fieldNames = {"key", "value"};
    private final TypeInformation[] typeInformations = {
            TypeInformation.of(String.class),
            TypeInformation.of(String.class)
    };

    /**
     * Test configure PulsarTableSink.
     *
     * @throws Exception
     */
    @Test
    public void testConfigure() throws Exception {
        PulsarJsonTableSink sink = spySink();

        TableSink<Row> configuredSink = sink.configure(fieldNames, typeInformations);

        Assert.assertArrayEquals(fieldNames, configuredSink.getFieldNames());
        Assert.assertArrayEquals(typeInformations, configuredSink.getFieldTypes());
        Assert.assertNotNull(((PulsarJsonTableSink) configuredSink).keyExtractor);
        Assert.assertNotNull(((PulsarJsonTableSink) configuredSink).serializationSchema);
    }

    /**
     * Test emit data stream.
     *
     * @throws Exception
     */
    @Test
    public void testEmitDataStream() throws Exception {
        DataStream mockedDataStream = Mockito.mock(DataStream.class);

        PulsarJsonTableSink sink = spySink();

        sink.emitDataStream(mockedDataStream);

        Mockito.verify(mockedDataStream).addSink(Mockito.any(FlinkPulsarProducer.class));
    }

    private PulsarJsonTableSink spySink() throws Exception {
        PulsarJsonTableSink sink = new PulsarJsonTableSink(SERVICE_URL, TOPIC_NAME, ROUTING_KEY);
        FlinkPulsarProducer producer = Mockito.mock(FlinkPulsarProducer.class);
        PowerMockito.whenNew(
                FlinkPulsarProducer.class
        ).withArguments(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(SerializationSchema.class),
                Mockito.any(PulsarKeyExtractor.class)
        ).thenReturn(producer);
        Whitebox.setInternalState(sink, "fieldNames", fieldNames);
        Whitebox.setInternalState(sink, "fieldTypes", typeInformations);
        Whitebox.setInternalState(sink, "serializationSchema", Mockito.mock(SerializationSchema.class));
        Whitebox.setInternalState(sink, "keyExtractor", Mockito.mock(PulsarKeyExtractor.class));
        return sink;
    }
}
