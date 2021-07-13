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
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.testing.TestingConnectorContext;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyCursorImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

public abstract class TestPulsarConnector {

    protected static final long currentTimeMs = 1534806330000L;

    protected PulsarConnectorConfig pulsarConnectorConfig;

    protected PulsarMetadata pulsarMetadata;

    protected PulsarAdmin pulsarAdmin;

    protected Schemas schemas;

    protected PulsarSplitManager pulsarSplitManager;

    protected Map<TopicName, PulsarRecordCursor> pulsarRecordCursors = new HashMap<>();

    protected static PulsarDispatchingRowDecoderFactory dispatchingRowDecoderFactory;

    protected static final PulsarConnectorId pulsarConnectorId = new PulsarConnectorId("test-connector");

    protected static List<TopicName> topicNames;
    protected static List<TopicName> partitionedTopicNames;
    protected static Map<String, Integer> partitionedTopicsToPartitions;
    protected static Map<String, SchemaInfo> topicsToSchemas;
    protected static Map<String, Long> topicsToNumEntries;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    protected static List<String> fooFieldNames = new ArrayList<>();

    protected static final NamespaceName NAMESPACE_NAME_1 = NamespaceName.get("tenant-1", "ns-1");
    protected static final NamespaceName NAMESPACE_NAME_2 = NamespaceName.get("tenant-1", "ns-2");
    protected static final NamespaceName NAMESPACE_NAME_3 = NamespaceName.get("tenant-2", "ns-1");
    protected static final NamespaceName NAMESPACE_NAME_4 = NamespaceName.get("tenant-2", "ns-2");

    protected static final TopicName TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-1");
    protected static final TopicName TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-2");
    protected static final TopicName TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2, "topic-1");
    protected static final TopicName TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3, "topic-1");
    protected static final TopicName TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-1");
    protected static final TopicName TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-2");
    protected static final TopicName NON_SCHEMA_TOPIC = TopicName.get(
        "persistent", NAMESPACE_NAME_2, "non-schema-topic");


    protected static final TopicName PARTITIONED_TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-2");
    protected static final TopicName PARTITIONED_TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-2");


    public static class Foo {
        public enum TestEnum {
            TEST_ENUM_1,
            TEST_ENUM_2,
            TEST_ENUM_3
        }

        public int field1;
        public String field2;
        public float field3;
        public double field4;
        public boolean field5;
        public long field6;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
        public long timestamp;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"time-millis\" }")
        public int time;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
        public int date;
        public TestPulsarConnector.Bar bar;
        public TestEnum field7;
    }

    public static class Bar {
        public Integer field1;
        public String field2;
        public float field3;
    }


    protected static Map<TopicName, List<PulsarColumnHandle>> topicsToColumnHandles = new HashMap<>();

    protected static Map<TopicName, PulsarSplit> splits;
    protected static Map<String, Function<Integer, Object>> fooFunctions;

    static {
        try {
            topicNames = new LinkedList<>();
            topicNames.add(TOPIC_1);
            topicNames.add(TOPIC_2);
            topicNames.add(TOPIC_3);
            topicNames.add(TOPIC_4);
            topicNames.add(TOPIC_5);
            topicNames.add(TOPIC_6);
            topicNames.add(NON_SCHEMA_TOPIC);

            partitionedTopicNames = new LinkedList<>();
            partitionedTopicNames.add(PARTITIONED_TOPIC_1);
            partitionedTopicNames.add(PARTITIONED_TOPIC_2);
            partitionedTopicNames.add(PARTITIONED_TOPIC_3);
            partitionedTopicNames.add(PARTITIONED_TOPIC_4);
            partitionedTopicNames.add(PARTITIONED_TOPIC_5);
            partitionedTopicNames.add(PARTITIONED_TOPIC_6);


            partitionedTopicsToPartitions = new HashMap<>();
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_1.toString(), 2);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_2.toString(), 3);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_3.toString(), 4);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_4.toString(), 5);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_5.toString(), 6);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_6.toString(), 7);

            topicsToSchemas = new HashMap<>();
            topicsToSchemas.put(TOPIC_1.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_2.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_3.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_4.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_5.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_6.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());

            topicsToSchemas.put(PARTITIONED_TOPIC_1.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_2.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_3.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_4.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_5.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_6.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());

            topicsToNumEntries = new HashMap<>();
            topicsToNumEntries.put(TOPIC_1.getSchemaName(), 1233L);
            topicsToNumEntries.put(TOPIC_2.getSchemaName(), 0L);
            topicsToNumEntries.put(TOPIC_3.getSchemaName(), 100L);
            topicsToNumEntries.put(TOPIC_4.getSchemaName(), 12345L);
            topicsToNumEntries.put(TOPIC_5.getSchemaName(), 8000L);
            topicsToNumEntries.put(TOPIC_6.getSchemaName(), 1L);

            topicsToNumEntries.put(NON_SCHEMA_TOPIC.getSchemaName(), 8000L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_1.getSchemaName(), 1233L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_2.getSchemaName(), 8000L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_3.getSchemaName(), 100L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_4.getSchemaName(), 0L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_5.getSchemaName(), 800L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_6.getSchemaName(), 1L);


            fooFieldNames.add("field1");
            fooFieldNames.add("field2");
            fooFieldNames.add("field3");
            fooFieldNames.add("field4");
            fooFieldNames.add("field5");
            fooFieldNames.add("field6");
            fooFieldNames.add("timestamp");
            fooFieldNames.add("time");
            fooFieldNames.add("date");
            fooFieldNames.add("bar");
            fooFieldNames.add("field7");


            ConnectorContext prestoConnectorContext = new TestingConnectorContext();
            dispatchingRowDecoderFactory = new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager());

            topicsToColumnHandles.put(PARTITIONED_TOPIC_1, getColumnColumnHandles(PARTITIONED_TOPIC_1,topicsToSchemas.get(PARTITIONED_TOPIC_1.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_2, getColumnColumnHandles(PARTITIONED_TOPIC_2,topicsToSchemas.get(PARTITIONED_TOPIC_2.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_3, getColumnColumnHandles(PARTITIONED_TOPIC_3,topicsToSchemas.get(PARTITIONED_TOPIC_3.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_4, getColumnColumnHandles(PARTITIONED_TOPIC_4,topicsToSchemas.get(PARTITIONED_TOPIC_4.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_5, getColumnColumnHandles(PARTITIONED_TOPIC_5,topicsToSchemas.get(PARTITIONED_TOPIC_5.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_6, getColumnColumnHandles(PARTITIONED_TOPIC_6,topicsToSchemas.get(PARTITIONED_TOPIC_6.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));


            topicsToColumnHandles.put(TOPIC_1, getColumnColumnHandles(TOPIC_1,topicsToSchemas.get(TOPIC_1.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(TOPIC_2, getColumnColumnHandles(TOPIC_2,topicsToSchemas.get(TOPIC_2.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(TOPIC_3, getColumnColumnHandles(TOPIC_3,topicsToSchemas.get(TOPIC_3.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(TOPIC_4, getColumnColumnHandles(TOPIC_4,topicsToSchemas.get(TOPIC_4.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(TOPIC_5, getColumnColumnHandles(TOPIC_5,topicsToSchemas.get(TOPIC_5.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));
            topicsToColumnHandles.put(TOPIC_6, getColumnColumnHandles(TOPIC_6,topicsToSchemas.get(TOPIC_6.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE,true));


            splits = new HashMap<>();

            List<TopicName> allTopics = new LinkedList<>();
            allTopics.addAll(topicNames);
            allTopics.addAll(partitionedTopicNames);


            for (TopicName topicName : allTopics) {
                if (topicsToSchemas.containsKey(topicName.getSchemaName())) {
                    splits.put(topicName, new PulsarSplit(0, pulsarConnectorId.toString(),
                        topicName.getNamespace(), topicName.getLocalName(), topicName.getLocalName(),
                        topicsToNumEntries.get(topicName.getSchemaName()),
                        new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema()),
                        topicsToSchemas.get(topicName.getSchemaName()).getType(),
                        0, topicsToNumEntries.get(topicName.getSchemaName()),
                        0, 0, TupleDomain.all(),
                            objectMapper.writeValueAsString(
                                    topicsToSchemas.get(topicName.getSchemaName()).getProperties()), null));
                }
            }

            fooFunctions = new HashMap<>();

            fooFunctions.put("field1", integer -> integer);
            fooFunctions.put("field2", String::valueOf);
            fooFunctions.put("field3", Integer::floatValue);
            fooFunctions.put("field4", Integer::doubleValue);
            fooFunctions.put("field5", integer -> integer % 2 == 0);
            fooFunctions.put("field6", Integer::longValue);
            fooFunctions.put("timestamp", integer -> System.currentTimeMillis());
            fooFunctions.put("time", integer -> {
                LocalTime now = LocalTime.now(ZoneId.systemDefault());
                return now.toSecondOfDay() * 1000;
            });
            fooFunctions.put("date", integer -> {
                LocalDate localDate = LocalDate.now();
                LocalDate epoch = LocalDate.ofEpochDay(0);
                return Math.toIntExact(ChronoUnit.DAYS.between(epoch, localDate));
            });
            fooFunctions.put("bar.field1", integer -> integer % 3 == 0 ? null : integer + 1);
            fooFunctions.put("bar.field2", integer -> integer % 2 == 0 ? null : String.valueOf(integer + 2));
            fooFunctions.put("bar.field3", integer -> integer + 3.0f);

            fooFunctions.put("field7", integer -> Foo.TestEnum.values()[integer % Foo.TestEnum.values().length]);

        } catch (Throwable e) {
            System.out.println("Error: " + e);
            System.out.println("Stacktrace: " + Arrays.asList(e.getStackTrace()));
        }
    }


    /**
     * Parse PulsarColumnMetadata to PulsarColumnHandle Util
     * @param schemaInfo
     * @param handleKeyValueType
     * @param includeInternalColumn
     * @param dispatchingRowDecoderFactory
     * @return
     */
    protected static List<PulsarColumnHandle> getColumnColumnHandles(TopicName topicName, SchemaInfo schemaInfo,
                                                                     PulsarColumnHandle.HandleKeyValueType handleKeyValueType, boolean includeInternalColumn) {
        List<PulsarColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadata = mockColumnMetadata().getPulsarColumns(topicName, schemaInfo,
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

    public static PulsarMetadata mockColumnMetadata() {
        ConnectorContext prestoConnectorContext = new TestingConnectorContext();
        PulsarConnectorConfig pulsarConnectorConfig = spy(new PulsarConnectorConfig());
        pulsarConnectorConfig.setMaxEntryReadBatchSize(1);
        pulsarConnectorConfig.setMaxSplitEntryQueueSize(10);
        pulsarConnectorConfig.setMaxSplitMessageQueueSize(100);
        PulsarDispatchingRowDecoderFactory dispatchingRowDecoderFactory =
                new PulsarDispatchingRowDecoderFactory(prestoConnectorContext.getTypeManager());
        PulsarMetadata pulsarMetadata = new PulsarMetadata(pulsarConnectorId, pulsarConnectorConfig, dispatchingRowDecoderFactory);
        return pulsarMetadata;
    }

    public static PulsarConnectorId getPulsarConnectorId() {
        assertNotNull(pulsarConnectorId);
        return pulsarConnectorId;
    }

    private static List<Entry> getTopicEntries(String topicSchemaName) {
        List<Entry> entries = new LinkedList<>();

        long count = topicsToNumEntries.get(topicSchemaName);
        for (int i=0 ; i < count; i++) {

            Foo foo = new Foo();
            foo.field1 = (int) count;
            foo.field2 = String.valueOf(count);
            foo.field3 = count;
            foo.field4 = count;
            foo.field5 = count % 2 == 0;
            foo.field6 = count;
            foo.timestamp = System.currentTimeMillis();

            LocalTime now = LocalTime.now(ZoneId.systemDefault());
            foo.time = now.toSecondOfDay() * 1000;

            LocalDate localDate = LocalDate.now();
            LocalDate epoch = LocalDate.ofEpochDay(0);
            foo.date = Math.toIntExact(ChronoUnit.DAYS.between(epoch, localDate));

            MessageMetadata messageMetadata = new MessageMetadata()
                    .setProducerName("test-producer").setSequenceId(i)
                    .setPublishTime(currentTimeMs + i);

            Schema schema = topicsToSchemas.get(topicSchemaName).getType() == SchemaType.AVRO ? AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build()) : JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

            ByteBuf payload = io.netty.buffer.Unpooled
                    .copiedBuffer(schema.encode(foo));

            ByteBuf byteBuf = serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, messageMetadata, payload);

            Entry entry = EntryImpl.create(0, i, byteBuf);
            log.info("create entry: %s", entry.getEntryId());
            entries.add(entry);
        }
        return entries;
    }

    public long completedBytes = 0L;

    private static final Logger log = Logger.get(TestPulsarConnector.class);

    protected static List<String> getNamespace(String tenant) {
        return topicNames.stream()
            .filter(topicName -> topicName.getTenant().equals(tenant))
            .map(TopicName::getNamespace)
            .distinct()
            .collect(Collectors.toCollection(LinkedList::new));
    }

    protected static List<String> getTopics(String ns) {
        List<String> topics = new ArrayList<>(topicNames.stream()
            .filter(topicName -> topicName.getNamespace().equals(ns))
            .map(TopicName::toString).collect(Collectors.toList()));
        partitionedTopicNames.stream().filter(topicName -> topicName.getNamespace().equals(ns)).forEach(topicName -> {
            for (Integer i = 0; i < partitionedTopicsToPartitions.get(topicName.toString()); i++) {
                topics.add(TopicName.get(topicName + "-partition-" + i).toString());
            }
        });
        return topics;
    }

    protected static List<String> getPartitionedTopics(String ns) {
        return partitionedTopicNames.stream()
            .filter(topicName -> topicName.getNamespace().equals(ns))
            .map(TopicName::toString)
            .collect(Collectors.toList());
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.pulsarConnectorConfig = spy(new PulsarConnectorConfig());
        this.pulsarConnectorConfig.setMaxEntryReadBatchSize(1);
        this.pulsarConnectorConfig.setMaxSplitEntryQueueSize(10);
        this.pulsarConnectorConfig.setMaxSplitMessageQueueSize(100);

        Tenants tenants = mock(Tenants.class);
        doReturn(new LinkedList<>(topicNames.stream()
            .map(TopicName::getTenant)
            .collect(Collectors.toSet()))).when(tenants).getTenants();

        Namespaces namespaces = mock(Namespaces.class);

        when(namespaces.getNamespaces(anyString())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String tenant = (String) args[0];
                List<String> ns = getNamespace(tenant);
                if (ns.isEmpty()) {
                    ClientErrorException cee = new ClientErrorException(Response.status(404).build());
                    throw new PulsarAdminException(cee, cee.getMessage(), cee.getResponse().getStatus());
                }
                return ns;
            }
        });

        Topics topics = mock(Topics.class);
        when(topics.getList(anyString(), any())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String ns = (String) args[0];
                List<String> topics = getTopics(ns);
                if (topics.isEmpty()) {
                    ClientErrorException cee = new ClientErrorException(Response.status(404).build());
                    throw new PulsarAdminException(cee, cee.getMessage(), cee.getResponse().getStatus());
                }
                return topics;
            }
        });

        when(topics.getPartitionedTopicList(anyString())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String ns = (String) args[0];
                List<String> topics = getPartitionedTopics(ns);
                if (topics.isEmpty()) {
                    ClientErrorException cee = new ClientErrorException(Response.status(404).build());
                    throw new PulsarAdminException(cee, cee.getMessage(), cee.getResponse().getStatus());
                }
                return topics;
            }
        });

        when(topics.getPartitionedTopicMetadata(anyString())).thenAnswer(new Answer<PartitionedTopicMetadata>() {
            @Override
            public PartitionedTopicMetadata answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String topic = (String) args[0];
                int partitions = partitionedTopicsToPartitions.get(topic) == null
                        ? 0 : partitionedTopicsToPartitions.get(topic);
                return new PartitionedTopicMetadata(partitions);
            }
        });

        schemas = mock(Schemas.class);
        when(schemas.getSchemaInfo(anyString())).thenAnswer(new Answer<SchemaInfo>() {
            @Override
            public SchemaInfo answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String topic = (String) args[0];
                if (topicsToSchemas.get(topic) != null) {
                    return topicsToSchemas.get(topic);
                } else {
                    ClientErrorException cee = new ClientErrorException(Response.status(404).build());
                    throw new PulsarAdminException(cee, cee.getMessage(), cee.getResponse().getStatus());
                }
            }
        });

        pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(tenants).when(pulsarAdmin).tenants();
        doReturn(namespaces).when(pulsarAdmin).namespaces();
        doReturn(topics).when(pulsarAdmin).topics();
        doReturn(schemas).when(pulsarAdmin).schemas();
        doReturn(pulsarAdmin).when(this.pulsarConnectorConfig).getPulsarAdmin();

        this.pulsarMetadata = new PulsarMetadata(pulsarConnectorId, this.pulsarConnectorConfig, dispatchingRowDecoderFactory);
        this.pulsarSplitManager = Mockito.spy(new PulsarSplitManager(pulsarConnectorId, this.pulsarConnectorConfig));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        when(managedLedgerFactory.openReadOnlyCursor(any(), any(), any())).then(new Answer<ReadOnlyCursor>() {

            private Map<String, Integer> positions = new HashMap<>();

            private int count = 0;
            @Override
            public ReadOnlyCursor answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String topic = (String) args[0];
                PositionImpl positionImpl = (PositionImpl) args[1];

                int position = positionImpl.getEntryId() == -1 ? 0 : (int) positionImpl.getEntryId();

                positions.put(topic, position);
                String schemaName = TopicName.get(
                        TopicName.get(
                                topic.replaceAll("/persistent", ""))
                                .getPartitionedTopicName()).getSchemaName();
                long entries = topicsToNumEntries.get(schemaName);


                ReadOnlyCursorImpl readOnlyCursor = mock(ReadOnlyCursorImpl.class);
                doReturn(entries).when(readOnlyCursor).getNumberOfEntries();

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
                                List <Entry> entries = new LinkedList<>();
                                for (int i = 0; i < readEntries; i++) {

                                    TestPulsarConnector.Bar bar = new TestPulsarConnector.Bar();
                                    bar.field1 = fooFunctions.get("bar.field1").apply(count) == null ? null : (int) fooFunctions.get("bar.field1").apply(count);
                                    bar.field2 = fooFunctions.get("bar.field2").apply(count) == null ? null : (String) fooFunctions.get("bar.field2").apply(count);
                                    bar.field3 = (float) fooFunctions.get("bar.field3").apply(count);


                                    Foo foo = new Foo();
                                    foo.field1 = (int) fooFunctions.get("field1").apply(count);
                                    foo.field2 = (String) fooFunctions.get("field2").apply(count);
                                    foo.field3 = (float) fooFunctions.get("field3").apply(count);
                                    foo.field4 = (double) fooFunctions.get("field4").apply(count);
                                    foo.field5 = (boolean) fooFunctions.get("field5").apply(count);
                                    foo.field6 = (long) fooFunctions.get("field6").apply(count);
                                    foo.timestamp = (long) fooFunctions.get("timestamp").apply(count);
                                    foo.time = (int) fooFunctions.get("time").apply(count);
                                    foo.date = (int) fooFunctions.get("date").apply(count);
                                    foo.bar = bar;
                                    foo.field7 = (Foo.TestEnum) fooFunctions.get("field7").apply(count);

                                    MessageMetadata messageMetadata = new MessageMetadata()
                                            .setProducerName("test-producer").setSequenceId(positions.get(topic))
                                            .setPublishTime(System.currentTimeMillis());

                                    Schema schema = topicsToSchemas.get(schemaName).getType() == SchemaType.AVRO ? AvroSchema.of(Foo.class) : JSONSchema.of(Foo.class);

                                    ByteBuf payload = io.netty.buffer.Unpooled
                                            .copiedBuffer(schema.encode(foo));

                                    ByteBuf byteBuf = serializeMetadataAndPayload(
                                            Commands.ChecksumType.Crc32c, messageMetadata, payload);

                                    completedBytes += byteBuf.readableBytes();

                                    entries.add(EntryImpl.create(0, positions.get(topic), byteBuf));
                                    positions.put(topic, positions.get(topic) + 1);
                                    count++;
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
                        return positions.get(topic) < entries;
                    }
                });

                when(readOnlyCursor.findNewestMatching(any(), any())).then(new Answer<Position>() {
                    @Override
                    public Position answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Object[] args = invocationOnMock.getArguments();
                        com.google.common.base.Predicate<Entry> predicate
                                = (com.google.common.base.Predicate<Entry>) args[1];

                        String schemaName = TopicName.get(
                                TopicName.get(
                                        topic.replaceAll("/persistent", ""))
                                        .getPartitionedTopicName()).getSchemaName();
                        List<Entry> entries = getTopicEntries(schemaName);

                        Integer target = null;
                        for (int i=entries.size() - 1; i >= 0; i--) {
                            Entry entry = entries.get(i);
                            if (predicate.apply(entry)) {
                                target = i;
                                break;
                            }
                        }

                        return target == null ? null : new PositionImpl(0, target);
                    }
                });

                when(readOnlyCursor.getNumberOfEntries(any())).then(new Answer<Long>() {
                    @Override
                    public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                        Object[] args = invocationOnMock.getArguments();
                        com.google.common.collect.Range<PositionImpl>  range
                                = (com.google.common.collect.Range<PositionImpl> ) args[0];

                        return (range.upperEndpoint().getEntryId() + 1) - range.lowerEndpoint().getEntryId();
                    }
                });

                when(readOnlyCursor.getCurrentLedgerInfo()).thenReturn(MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder().setLedgerId(0).build());

                return readOnlyCursor;
            }
        });

        PulsarConnectorCache.instance = mock(PulsarConnectorCache.class);
        when(PulsarConnectorCache.instance.getManagedLedgerFactory()).thenReturn(managedLedgerFactory);

        for (Map.Entry<TopicName, PulsarSplit> split : splits.entrySet()) {
            PulsarRecordCursor pulsarRecordCursor = spy(new PulsarRecordCursor(
                    topicsToColumnHandles.get(split.getKey()), split.getValue(),
                    pulsarConnectorConfig, managedLedgerFactory, new ManagedLedgerConfig(),
                    new PulsarConnectorMetricsTracker(new NullStatsProvider()),dispatchingRowDecoderFactory));
            this.pulsarRecordCursors.put(split.getKey(), pulsarRecordCursor);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        completedBytes = 0L;
    }

    @DataProvider(name = "rewriteNamespaceDelimiter")
    public static Object[][] serviceUrls() {
        return new Object[][] {
                { "|" }, { null }
        };
    }

    protected void updateRewriteNamespaceDelimiterIfNeeded(String delimiter) {
        if (StringUtils.isNotBlank(delimiter)) {
            pulsarConnectorConfig.setNamespaceDelimiterRewriteEnable(true);
            pulsarConnectorConfig.setRewriteNamespaceDelimiter(delimiter);
        } else {
            pulsarConnectorConfig.setNamespaceDelimiterRewriteEnable(false);
        }
    }
}
