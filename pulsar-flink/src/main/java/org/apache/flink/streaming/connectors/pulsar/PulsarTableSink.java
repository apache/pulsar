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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import java.util.Arrays;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarPropertiesExtractor;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

/**
 * An append-only table sink to emit a streaming table as a Pulsar stream.
 */
public abstract class PulsarTableSink implements AppendStreamTableSink<Row> {

    protected ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
    protected ProducerConfigurationData producerConfigurationData = new ProducerConfigurationData();
    protected SerializationSchema<Row> serializationSchema;
    protected PulsarKeyExtractor<Row> keyExtractor;
    protected PulsarPropertiesExtractor<Row> propertiesExtractor;
    protected String[] fieldNames;
    protected TypeInformation[] fieldTypes;
    protected final String routingKeyFieldName;

    public PulsarTableSink(
            String serviceUrl,
            String topic,
            Authentication authentication,
            String routingKeyFieldName) {
        checkNotNull(serviceUrl, "Service url not set");
        checkNotNull(topic, "Topic is null");
        this.clientConfigurationData.setServiceUrl(serviceUrl);
        this.clientConfigurationData.setAuthentication(authentication);
        this.producerConfigurationData.setTopicName(topic);
        this.routingKeyFieldName = routingKeyFieldName;
    }

    public PulsarTableSink(
            ClientConfigurationData clientConfigurationData,
            ProducerConfigurationData producerConfigurationData,
            String routingKeyFieldName) {
        this.clientConfigurationData = checkNotNull(clientConfigurationData, "client config is null");
        this.producerConfigurationData = checkNotNull(producerConfigurationData, "producer config is null");
        this.routingKeyFieldName = routingKeyFieldName;
    }

    /**
     * Create serialization schema for converting table rows into bytes.
     *
     * @param rowSchema the schema of the row to serialize.
     * @return Instance of serialization schema
     */
    protected abstract SerializationSchema<Row> createSerializationSchema(RowTypeInfo rowSchema);

    /**
     * Create a deep copy of this sink.
     *
     * @return Deep copy of this sink
     */
    protected abstract PulsarTableSink createSink();

    /**
     * Returns the low-level producer.
     */
    protected FlinkPulsarProducer<Row> createFlinkPulsarProducer() {
        return new FlinkPulsarProducer<>(
            clientConfigurationData,
            producerConfigurationData,
            serializationSchema,
            keyExtractor,
            propertiesExtractor);
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        checkState(fieldNames != null, "Table sink is not configured");
        checkState(fieldTypes != null, "Table sink is not configured");
        checkState(serializationSchema != null, "Table sink is not configured");
        checkState(keyExtractor != null, "Table sink is not configured");

        FlinkPulsarProducer<Row> producer = createFlinkPulsarProducer();
        dataStream.addSink(producer);
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames,
                                    TypeInformation<?>[] fieldTypes) {

        PulsarTableSink sink = createSink();

        sink.fieldNames = checkNotNull(fieldNames, "Field names are null");
        sink.fieldTypes = checkNotNull(fieldTypes, "Field types are null");
        checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types do not match");

        RowTypeInfo rowSchema = new RowTypeInfo(fieldTypes, fieldNames);
        sink.serializationSchema = createSerializationSchema(rowSchema);
        sink.keyExtractor = new RowKeyExtractor(
                routingKeyFieldName,
                fieldNames,
                fieldTypes);
        sink.propertiesExtractor = PulsarPropertiesExtractor.EMPTY;

        return sink;
    }

    /**
     * A key extractor that extracts the routing key from a {@link Row} by field name.
     */
    private static class RowKeyExtractor implements PulsarKeyExtractor<Row> {

        private final int keyIndex;

        public RowKeyExtractor(
                String keyFieldName,
                String[] fieldNames,
                TypeInformation<?>[] fieldTypes) {
            checkArgument(fieldNames.length == fieldTypes.length,
                    "Number of provided field names and types does not match.");
            int keyIndex = Arrays.asList(fieldNames).indexOf(keyFieldName);
            checkArgument(keyIndex >= 0,
                    "Key field '" + keyFieldName + "' not found");
            checkArgument(Types.STRING.equals(fieldTypes[keyIndex]),
                    "Key field must be of type 'STRING'");
            this.keyIndex = keyIndex;
        }

        @Override
        public String getKey(Row event) {
            return (String) event.getField(keyIndex);
        }
    }
}
