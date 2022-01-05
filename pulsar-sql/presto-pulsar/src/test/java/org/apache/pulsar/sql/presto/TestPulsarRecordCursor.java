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
package org.apache.pulsar.sql.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.RowType;
import lombok.Data;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyCursorImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPulsarRecordCursor extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarRecordCursor.class);

    @Test(singleThreaded = true)
    public void testTopics() throws Exception {

        for (Map.Entry<TopicName, PulsarRecordCursor> entry : pulsarRecordCursors.entrySet()) {

            log.info("!------ topic %s ------!", entry.getKey());
            setup();

            List<PulsarColumnHandle> fooColumnHandles = topicsToColumnHandles.get(entry.getKey());
            PulsarRecordCursor pulsarRecordCursor = entry.getValue();

            PulsarSqlSchemaInfoProvider pulsarSqlSchemaInfoProvider = mock(PulsarSqlSchemaInfoProvider.class);
            when(pulsarSqlSchemaInfoProvider.getSchemaByVersion(any())).thenReturn(completedFuture(topicsToSchemas.get(entry.getKey().getSchemaName())));
            pulsarRecordCursor.setPulsarSqlSchemaInfoProvider(pulsarSqlSchemaInfoProvider);

            TopicName topicName = entry.getKey();

            int count = 0;
            while (pulsarRecordCursor.advanceNextPosition()) {
                List<String> columnsSeen = new LinkedList<>();
                for (int i = 0; i < fooColumnHandles.size(); i++) {
                    if (pulsarRecordCursor.isNull(i)) {
                        columnsSeen.add(fooColumnHandles.get(i).getName());
                    } else {
                        if (fooColumnHandles.get(i).getName().equals("field1")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Integer) fooFunctions.get("field1").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field2")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), ((String) fooFunctions.get("field2").apply(count)).getBytes());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field3")) {
                            assertEquals(pulsarRecordCursor.getLong(i), Float.floatToIntBits((Float) fooFunctions.get("field3").apply(count)));
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field4")) {
                            assertEquals(pulsarRecordCursor.getDouble(i), ((Double) fooFunctions.get("field4").apply(count)).doubleValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field5")) {
                            assertEquals(pulsarRecordCursor.getBoolean(i), ((Boolean) fooFunctions.get("field5").apply(count)).booleanValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("field6")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Long) fooFunctions.get("field6").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("timestamp")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("time")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("date")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else if (fooColumnHandles.get(i).getName().equals("bar")) {
                            assertTrue(fooColumnHandles.get(i).getType() instanceof RowType);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }else if (fooColumnHandles.get(i).getName().equals("field7")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), fooFunctions.get("field7").apply(count).toString().getBytes());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        } else {
                            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(fooColumnHandles.get(i).getName())) {
                                columnsSeen.add(fooColumnHandles.get(i).getName());
                            }
                        }
                    }
                }
                assertEquals(columnsSeen.size(), fooColumnHandles.size());
                count++;
            }
            assertEquals(count, topicsToNumEntries.get(topicName.getSchemaName()).longValue());
            assertEquals(pulsarRecordCursor.getCompletedBytes(), completedBytes);
            cleanup();
            pulsarRecordCursor.close();
        }
    }

    @Test(singleThreaded = true)
    public void TestKeyValueStructSchema() throws Exception {

        TopicName topicName = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-4");
        Long entriesNum = 5L;

        for (KeyValueEncodingType encodingType :
                Arrays.asList(KeyValueEncodingType.INLINE, KeyValueEncodingType.SEPARATED)) {

            KeyValueSchemaImpl schema = (KeyValueSchemaImpl) Schema.KeyValue(Schema.JSON(Foo.class), Schema.AVRO(Boo.class),
                    encodingType);

            Foo foo = new Foo();
            foo.field1 = "field1-value";
            foo.field2 = 20;
            Boo boo = new Boo();
            boo.field1 = "field1-value";
            boo.field2 = true;
            boo.field3 = 10.2;

            KeyValue message = new KeyValue<>(foo, boo);
            List<PulsarColumnHandle> ColumnHandles = getColumnColumnHandles(topicName, schema.getSchemaInfo(), PulsarColumnHandle.HandleKeyValueType.NONE, true);
            PulsarRecordCursor pulsarRecordCursor = mockKeyValueSchemaPulsarRecordCursor(entriesNum, topicName,
                    schema, message, ColumnHandles);

            assertNotNull(pulsarRecordCursor);
            Long count = 0L;
            while (pulsarRecordCursor.advanceNextPosition()) {
                List<String> columnsSeen = new LinkedList<>();
                for (int i = 0; i < ColumnHandles.size(); i++) {
                    if (pulsarRecordCursor.isNull(i)) {
                        columnsSeen.add(ColumnHandles.get(i).getName());
                    } else {
                        if (ColumnHandles.get(i).getName().equals("field1")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), boo.field1.getBytes());
                            columnsSeen.add(ColumnHandles.get(i).getName());
                        } else if (ColumnHandles.get(i).getName().equals("field2")) {
                            assertEquals(pulsarRecordCursor.getBoolean(i), boo.field2.booleanValue());
                            columnsSeen.add(ColumnHandles.get(i).getName());
                        } else if (ColumnHandles.get(i).getName().equals("field3")) {
                            assertEquals((Double) pulsarRecordCursor.getDouble(i), (Double) boo.field3);
                            columnsSeen.add(ColumnHandles.get(i).getName());
                        } else if (ColumnHandles.get(i).getName().equals(PulsarColumnMetadata.KEY_SCHEMA_COLUMN_PREFIX +
                                "field1")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), foo.field1.getBytes());
                            columnsSeen.add(ColumnHandles.get(i).getName());
                        } else if (ColumnHandles.get(i).getName().equals(PulsarColumnMetadata.KEY_SCHEMA_COLUMN_PREFIX +
                                "field2")) {
                            assertEquals(pulsarRecordCursor.getLong(i), Long.valueOf(foo.field2).longValue());
                            columnsSeen.add(ColumnHandles.get(i).getName());
                        } else {
                            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(ColumnHandles.get(i).getName())) {
                                columnsSeen.add(ColumnHandles.get(i).getName());
                            }
                        }
                    }
                }
                assertEquals(columnsSeen.size(), ColumnHandles.size());
                count++;
            }
            assertEquals(count, entriesNum);
            pulsarRecordCursor.close();
        }
    }

    @Test(singleThreaded = true)
    public void TestKeyValuePrimitiveSchema() throws Exception {

        TopicName topicName = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-4");
        Long entriesNum = 5L;

        for (KeyValueEncodingType encodingType :
                Arrays.asList(KeyValueEncodingType.INLINE, KeyValueEncodingType.SEPARATED)) {

            KeyValueSchemaImpl schema = (KeyValueSchemaImpl) Schema.KeyValue(Schema.INT32, Schema.STRING,
                    encodingType);

            String value = "primitive_message_value";
            Integer key = 23;
            KeyValue message = new KeyValue<>(key, value);

            List<PulsarColumnHandle> ColumnHandles = getColumnColumnHandles(topicName, schema.getSchemaInfo(), PulsarColumnHandle.HandleKeyValueType.NONE, true);
            PulsarRecordCursor pulsarRecordCursor = mockKeyValueSchemaPulsarRecordCursor(entriesNum, topicName,
                    schema, message, ColumnHandles);

            assertNotNull(pulsarRecordCursor);
            Long count = 0L;
            while (pulsarRecordCursor.advanceNextPosition()) {
                List<String> columnsSeen = new LinkedList<>();
                for (int i = 0; i < ColumnHandles.size(); i++) {
                    if (pulsarRecordCursor.isNull(i)) {
                        columnsSeen.add(ColumnHandles.get(i).getName());
                    } else {
                        if (ColumnHandles.get(i).getName().equals(PRIMITIVE_COLUMN_NAME)) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), value.getBytes());
                            columnsSeen.add(ColumnHandles.get(i).getName());
                        } else if (ColumnHandles.get(i).getName().equals(KEY_SCHEMA_COLUMN_PREFIX +
                                PRIMITIVE_COLUMN_NAME)) {
                            assertEquals((Long) pulsarRecordCursor.getLong(i), Long.valueOf(key));
                            columnsSeen.add(ColumnHandles.get(i).getName());
                        } else {
                            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(ColumnHandles.get(i).getName())) {
                                columnsSeen.add(ColumnHandles.get(i).getName());
                            }
                        }
                    }
                }
                assertEquals(columnsSeen.size(), ColumnHandles.size());
                count++;
            }
            assertEquals(count, entriesNum);
            pulsarRecordCursor.close();
        }
    }


    /**
     * mock a simple PulsarRecordCursor for KeyValueSchema test.
     * @param entriesNum
     * @param topicName
     * @param schema
     * @param message
     * @param ColumnHandles
     * @return
     * @throws Exception
     */
    private PulsarRecordCursor mockKeyValueSchemaPulsarRecordCursor(final Long entriesNum, final TopicName topicName,
                                                                    final KeyValueSchemaImpl schema, KeyValue message, List<PulsarColumnHandle> ColumnHandles) throws Exception {

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);

        when(managedLedgerFactory.openReadOnlyCursor(any(), any(), any())).then(new Answer<ReadOnlyCursor>() {

            private Map<String, Integer> positions = new HashMap<>();

            @Override
            public ReadOnlyCursor answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String topic = (String) args[0];
                PositionImpl positionImpl = (PositionImpl) args[1];
                int position = positionImpl.getEntryId() == -1 ? 0 : (int) positionImpl.getEntryId();

                positions.put(topic, position);
                ReadOnlyCursorImpl readOnlyCursor = mock(ReadOnlyCursorImpl.class);
                doReturn(entriesNum).when(readOnlyCursor).getNumberOfEntries();

                doAnswer(new Answer<Void>() {
                    @Override
                    public Void answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        Integer skipEntries = (Integer) args[0];
                        positions.put(topic, positions.get(topic) + skipEntries);
                        return null;
                    }
                }).when(readOnlyCursor).skipEntries(anyInt());

                when(readOnlyCursor.getReadPosition()).thenAnswer(new Answer<PositionImpl>() {
                    @Override
                    public PositionImpl answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return PositionImpl.get(0, positions.get(topic));
                    }
                });

                doAnswer(new Answer() {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Object[] args = invocationOnMock.getArguments();
                        Integer readEntries = (Integer) args[0];
                        AsyncCallbacks.ReadEntriesCallback callback = (AsyncCallbacks.ReadEntriesCallback) args[2];
                        Object ctx = args[3];

                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                List<Entry> entries = new LinkedList<>();
                                for (int i = 0; i < readEntries; i++) {

                                    MessageMetadata messageMetadata =
                                            new MessageMetadata()
                                                    .setProducerName("test-producer")
                                                    .setSequenceId(positions.get(topic))
                                                    .setPublishTime(System.currentTimeMillis());

                                    if (i % 2 == 0) {
                                        messageMetadata.setSchemaVersion(new LongSchemaVersion(1L).bytes());
                                    }

                                    if (KeyValueEncodingType.SEPARATED.equals(schema.getKeyValueEncodingType())) {
                                        messageMetadata
                                                .setPartitionKey(new String(schema
                                                        .getKeySchema().encode(message.getKey()), Charset.forName(
                                                        "UTF-8")))
                                                .setPartitionKeyB64Encoded(false);
                                    }

                                    ByteBuf dataPayload = io.netty.buffer.Unpooled
                                            .copiedBuffer(schema.encode(message));

                                    ByteBuf byteBuf = serializeMetadataAndPayload(
                                            Commands.ChecksumType.Crc32c, messageMetadata, dataPayload);

                                    entries.add(EntryImpl.create(0, positions.get(topic), byteBuf));
                                    positions.put(topic, positions.get(topic) + 1);
                                }

                                callback.readEntriesComplete(entries, ctx);
                            }
                        }).start();

                        return null;
                    }
                }).when(readOnlyCursor).asyncReadEntries(anyInt(), anyLong(), any(), any(), any());

                when(readOnlyCursor.hasMoreEntries()).thenAnswer(new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                        return positions.get(topic) < entriesNum;
                    }
                });

                when(readOnlyCursor.getNumberOfEntries(any())).then(new Answer<Long>() {
                    @Override
                    public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Object[] args = invocationOnMock.getArguments();
                        com.google.common.collect.Range<PositionImpl> range
                                = (com.google.common.collect.Range<PositionImpl>) args[0];
                        return (range.upperEndpoint().getEntryId() + 1) - range.lowerEndpoint().getEntryId();
                    }
                });

                when(readOnlyCursor.getCurrentLedgerInfo()).thenReturn(MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder().setLedgerId(0).build());

                return readOnlyCursor;
            }
        });

        ObjectMapper objectMapper = new ObjectMapper();

        PulsarSplit split = new PulsarSplit(0, pulsarConnectorId.toString(),
                topicName.getNamespace(), topicName.getLocalName(), topicName.getLocalName(),
                entriesNum,
                new String(schema.getSchemaInfo().getSchema(),  "ISO8859-1"),
                schema.getSchemaInfo().getType(),
                0, entriesNum,
                0, 0, TupleDomain.all(),
                objectMapper.writeValueAsString(
                        schema.getSchemaInfo().getProperties()), null);

        PulsarRecordCursor pulsarRecordCursor = spy(new PulsarRecordCursor(
                ColumnHandles, split,
                pulsarConnectorConfig, managedLedgerFactory, new ManagedLedgerConfig(),
                new PulsarConnectorMetricsTracker(new NullStatsProvider()), dispatchingRowDecoderFactory));

        PulsarSqlSchemaInfoProvider pulsarSqlSchemaInfoProvider = mock(PulsarSqlSchemaInfoProvider.class);
        when(pulsarSqlSchemaInfoProvider.getSchemaByVersion(any())).thenReturn(completedFuture(schema.getSchemaInfo()));
        pulsarRecordCursor.setPulsarSqlSchemaInfoProvider(pulsarSqlSchemaInfoProvider);

        return pulsarRecordCursor;
    }


    static final String KEY_SCHEMA_COLUMN_PREFIX = "__key.";
    static final String PRIMITIVE_COLUMN_NAME = "__value__";

    @Data
    static class Foo {
        private String field1;
        private Integer field2;
    }

    @Data
    static class Boo {
        private String field1;
        private Boolean field2;
        private Double field3;
    }

    @Test
    public void testGetSchemaInfo() throws Exception {
        String topic = "get-schema-test";
        PulsarSplit pulsarSplit = Mockito.mock(PulsarSplit.class);
        Mockito.when(pulsarSplit.getTableName()).thenReturn(TopicName.get(topic).getLocalName());
        Mockito.when(pulsarSplit.getSchemaName()).thenReturn("public/default");
        PulsarAdmin pulsarAdmin = Mockito.mock(PulsarAdmin.class);
        Schemas schemas = Mockito.mock(Schemas.class);
        Mockito.when(pulsarAdmin.schemas()).thenReturn(schemas);
        PulsarConnectorConfig connectorConfig = spy(PulsarConnectorConfig.class);
        Mockito.when(connectorConfig.getPulsarAdmin()).thenReturn(pulsarAdmin);
        PulsarRecordCursor pulsarRecordCursor = spy(new PulsarRecordCursor(
                new ArrayList<>(), pulsarSplit, connectorConfig, Mockito.mock(ManagedLedgerFactory.class),
                new ManagedLedgerConfig(), null, null));

        Class<PulsarRecordCursor> clazz =  PulsarRecordCursor.class;
        Method getSchemaInfo = clazz.getDeclaredMethod("getSchemaInfo", PulsarSplit.class);
        getSchemaInfo.setAccessible(true);
        Field currentMessage = clazz.getDeclaredField("currentMessage");
        currentMessage.setAccessible(true);
        RawMessage rawMessage = Mockito.mock(RawMessage.class);
        currentMessage.set(pulsarRecordCursor, rawMessage);

        // If the schemaType of pulsarSplit is NONE or BYTES, using bytes schema
        Mockito.when(pulsarSplit.getSchemaType()).thenReturn(SchemaType.NONE);
        SchemaInfo schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
        assertEquals(SchemaType.BYTES, schemaInfo.getType());

        Mockito.when(pulsarSplit.getSchemaType()).thenReturn(SchemaType.BYTES);
        schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
        assertEquals(SchemaType.BYTES, schemaInfo.getType());

        Mockito.when(pulsarSplit.getSchemaName()).thenReturn(Schema.BYTEBUFFER.getSchemaInfo().getName());
        schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
        assertEquals(SchemaType.BYTES, schemaInfo.getType());

        // If the schemaVersion of the message is not null, try to get the schema.
        Mockito.when(pulsarSplit.getSchemaType()).thenReturn(SchemaType.AVRO);
        Mockito.when(rawMessage.getSchemaVersion()).thenReturn(new LongSchemaVersion(0).bytes());
        Mockito.when(schemas.getSchemaInfo(anyString(), eq(0L)))
                .thenReturn(Schema.AVRO(Foo.class).getSchemaInfo());
        schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
        assertEquals(SchemaType.AVRO, schemaInfo.getType());

        String schemaTopic = "persistent://public/default/" + topic;

        // If the schemaVersion of the message is null and the schema of pulsarSplit is null, throw runtime exception.
        Mockito.when(pulsarSplit.getSchemaInfo()).thenReturn(null);
        Mockito.when(rawMessage.getSchemaVersion()).thenReturn(null);
        try {
            schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
            fail("The message schema version is null and the latest schema is null, should fail.");
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().contains("schema of the table " + topic + " is null"));
        }

        // If the schemaVersion of the message is null, try to get the latest schema.
        Mockito.when(rawMessage.getSchemaVersion()).thenReturn(null);
        Mockito.when(pulsarSplit.getSchemaInfo()).thenReturn(Schema.AVRO(Foo.class).getSchemaInfo());
        schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
        assertEquals(Schema.AVRO(Foo.class).getSchemaInfo(), schemaInfo);

        // If the specific version schema is null, throw runtime exception.
        Mockito.when(rawMessage.getSchemaVersion()).thenReturn(new LongSchemaVersion(1L).bytes());
        Mockito.when(schemas.getSchemaInfo(schemaTopic, 1)).thenReturn(null);
        try {
            schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
            fail("The specific version " + 1 + " schema is null, should fail.");
        } catch (InvocationTargetException e) {
            String schemaVersion = BytesSchemaVersion.of(new LongSchemaVersion(1L).bytes()).toString();
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().contains("schema of the topic " + schemaTopic + " is null"));
        }

        // Get the specific version schema.
        Mockito.when(rawMessage.getSchemaVersion()).thenReturn(new LongSchemaVersion(2L).bytes());
        Mockito.when(schemas.getSchemaInfo(schemaTopic, 2)).thenReturn(Schema.AVRO(Foo.class).getSchemaInfo());
        schemaInfo = (SchemaInfo) getSchemaInfo.invoke(pulsarRecordCursor, pulsarSplit);
        assertEquals(Schema.AVRO(Foo.class).getSchemaInfo(), schemaInfo);
    }

}
