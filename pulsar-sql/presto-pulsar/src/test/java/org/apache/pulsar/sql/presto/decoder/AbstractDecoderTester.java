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
package org.apache.pulsar.sql.presto.decoder;

import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingConnectorContext;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.sql.presto.PulsarColumnHandle;
import org.apache.pulsar.sql.presto.PulsarColumnMetadata;
import org.apache.pulsar.sql.presto.PulsarConnectorConfig;
import org.apache.pulsar.sql.presto.PulsarConnectorId;
import org.apache.pulsar.sql.presto.PulsarDispatchingRowDecoderFactory;
import org.apache.pulsar.sql.presto.PulsarMetadata;
import org.apache.pulsar.sql.presto.PulsarRowDecoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertNotNull;

/**
 * Abstract superclass for TestXXDecoder (e.g. TestAvroDecoder 、TestJsonDecoder).
 */
public abstract class AbstractDecoderTester {

    protected PulsarDispatchingRowDecoderFactory decoderFactory;
    protected PulsarConnectorId pulsarConnectorId = new PulsarConnectorId("test-connector");
    protected SchemaInfo schemaInfo;
    protected TopicName topicName;
    protected List<PulsarColumnHandle> pulsarColumnHandle;
    protected PulsarRowDecoder pulsarRowDecoder;
    protected DecoderTestUtil decoderTestUtil;
    protected PulsarConnectorConfig pulsarConnectorConfig;
    protected PulsarMetadata pulsarMetadata;

    protected void init() {
        ConnectorContext prestoConnectorContext = new TestingConnectorContext();
        this.decoderFactory = new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager());
        this.pulsarConnectorConfig = spy(new PulsarConnectorConfig());
        this.pulsarConnectorConfig.setMaxEntryReadBatchSize(1);
        this.pulsarConnectorConfig.setMaxSplitEntryQueueSize(10);
        this.pulsarConnectorConfig.setMaxSplitMessageQueueSize(100);
        this.pulsarMetadata = new PulsarMetadata(pulsarConnectorId, this.pulsarConnectorConfig, decoderFactory);
        this.topicName = TopicName.get("persistent", NamespaceName.get("tenant-1", "ns-1"), "topic-1");
    }

    protected void checkArrayValues(Block block, Type type, Object value) {
        decoderTestUtil.checkArrayValues(block, type, value);
    }

    protected void checkMapValues(Block block, Type type, Object value) {
        decoderTestUtil.checkMapValues(block, type, value);
    }

    protected void checkRowValues(Block block, Type type, Object value) {
        decoderTestUtil.checkRowValues(block, type, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, Slice value) {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, String value) {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, long value) {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, double value) {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected void checkValue(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle, boolean value) {
        decoderTestUtil.checkValue(decodedRow, handle, value);
    }

    protected Block getBlock(Map<DecoderColumnHandle, FieldValueProvider> decodedRow, DecoderColumnHandle handle) {
        FieldValueProvider provider = decodedRow.get(handle);
        assertNotNull(provider);
        return provider.getBlock();
    }

    protected List<PulsarColumnHandle> getColumnColumnHandles(TopicName topicName, SchemaInfo schemaInfo,
                                                              PulsarColumnHandle.HandleKeyValueType handleKeyValueType, boolean includeInternalColumn, PulsarDispatchingRowDecoderFactory dispatchingRowDecoderFactory) {
        List<PulsarColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadata = pulsarMetadata.getPulsarColumns(topicName, schemaInfo,
                includeInternalColumn, handleKeyValueType);

        columnMetadata.forEach(column -> {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) column;
            columnHandles.add(new PulsarColumnHandle(
                    pulsarConnectorId.toString(),
                    pulsarColumnMetadata.getNameWithCase(),
                    pulsarColumnMetadata.getType(),
                    pulsarColumnMetadata.isHidden(),
                    pulsarColumnMetadata.isInternal(),
                    pulsarColumnMetadata.getDecoderExtraInfo().getMapping(),
                    pulsarColumnMetadata.getDecoderExtraInfo().getDataFormat(), pulsarColumnMetadata.getDecoderExtraInfo().getFormatHint(),
                    pulsarColumnMetadata.getHandleKeyValueType()));

        });
        return columnHandles;
    }

}
