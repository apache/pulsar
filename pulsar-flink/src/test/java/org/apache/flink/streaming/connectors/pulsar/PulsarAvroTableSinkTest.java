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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.avro.generated.NasaMission;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarPropertiesExtractor;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

/**
 * Unit test of {@link PulsarAvroTableSink}.
 */
public class PulsarAvroTableSinkTest {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC_NAME = "test_topic";
    private static final Authentication AUTHENTICATION = new AuthenticationDisabled();
    private static final String ROUTING_KEY = "name";

    private final String[] fieldNames = {"id", "name","start_year","end_year"};
    private final TypeInformation[] typeInformations = {
            TypeInformation.of(Integer.class),
            TypeInformation.of(String.class),
            TypeInformation.of(Integer.class),
            TypeInformation.of(Integer.class)
    };

    /**
     * Test configure PulsarTableSink.
     *
     * @throws Exception
     */
    @Test
    public void testConfigure() throws Exception {
        PulsarAvroTableSink sink = spySink();

        TableSink<Row> configuredSink = sink.configure(fieldNames, typeInformations);

        assertArrayEquals(fieldNames, configuredSink.getFieldNames());
        assertArrayEquals(typeInformations, configuredSink.getFieldTypes());
        assertNotNull(((PulsarAvroTableSink) configuredSink).keyExtractor);
        assertNotNull(((PulsarAvroTableSink) configuredSink).serializationSchema);
    }


    /**
     * Test emit data stream.
     *
     * @throws Exception
     */
    @Test
    public void testEmitDataStream() throws Exception {
        DataStream mockedDataStream = Mockito.mock(DataStream.class);

        PulsarAvroTableSink sink = spySink();

        sink.emitDataStream(mockedDataStream);

        Mockito.verify(mockedDataStream).addSink(Mockito.any(FlinkPulsarProducer.class));
    }


    private PulsarAvroTableSink spySink() throws Exception {

        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(SERVICE_URL);

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(TOPIC_NAME);

        PulsarAvroTableSink sink =
                new PulsarAvroTableSink(clientConf, producerConf, ROUTING_KEY, NasaMission.class);
        FlinkPulsarProducer producer = Mockito.mock(FlinkPulsarProducer.class);
        PowerMockito.whenNew(
                FlinkPulsarProducer.class
        ).withArguments(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(Authentication.class),
                Mockito.any(SerializationSchema.class),
                Mockito.any(PulsarKeyExtractor.class),
                Mockito.any(PulsarPropertiesExtractor.class)
        ).thenReturn(producer);
        FieldUtils.writeField(sink, "fieldNames", fieldNames, true);
        FieldUtils.writeField(sink, "fieldTypes", typeInformations, true);
        FieldUtils.writeField(sink, "serializationSchema", Mockito.mock(SerializationSchema.class), true);
        FieldUtils.writeField(sink, "keyExtractor", Mockito.mock(PulsarKeyExtractor.class), true);
        FieldUtils.writeField(sink, "propertiesExtractor", Mockito.mock(PulsarPropertiesExtractor.class), true);
        return sink;
    }

}
