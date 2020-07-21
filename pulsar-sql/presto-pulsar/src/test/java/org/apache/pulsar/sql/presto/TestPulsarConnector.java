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
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
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
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class TestPulsarConnector {

    protected static final long currentTimeMs = 1534806330000L;

    protected PulsarConnectorConfig pulsarConnectorConfig;

    protected PulsarMetadata pulsarMetadata;

    protected PulsarAdmin pulsarAdmin;

    protected Schemas schemas;

    protected PulsarSplitManager pulsarSplitManager;

    protected Map<TopicName, PulsarRecordCursor> pulsarRecordCursors = new HashMap<>();

    protected final static PulsarConnectorId pulsarConnectorId = new PulsarConnectorId("test-connector");

    protected static List<TopicName> topicNames;
    protected static List<TopicName> partitionedTopicNames;
    protected static Map<String, Integer> partitionedTopicsToPartitions;
    protected static Map<String, SchemaInfo> topicsToSchemas;
    protected static Map<String, Long> topicsToNumEntries;

    private final static ObjectMapper objectMapper = new ObjectMapper();

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
        public static class Bar {
            public int field1;
        }

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
        public Boo test;
        public float field3;
        public Boo test2;
    }

    public static class Boo {
        public Double field4;
        public Boolean field5;
        public long field6;
        // for test cyclic definitions
        public Foo foo;
        public Boo boo;
        public Bar bar;
        // different namespace with same classname should work though
        public Foo.Bar foobar;
    }

    protected static Map<String, Type> fooTypes;
    protected static List<PulsarColumnHandle> fooColumnHandles;
    protected static Map<TopicName, PulsarSplit> splits;
    protected static Map<String, String[]> fooFieldNames;
    protected static Map<String, Integer[]> fooPositionIndices;
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

            fooTypes = new HashMap<>();
            fooTypes.put("field1", IntegerType.INTEGER);
            fooTypes.put("field2", VarcharType.VARCHAR);
            fooTypes.put("field3", RealType.REAL);
            fooTypes.put("field4", DoubleType.DOUBLE);
            fooTypes.put("field5", BooleanType.BOOLEAN);
            fooTypes.put("field6", BigintType.BIGINT);
            fooTypes.put("timestamp", TIMESTAMP);
            fooTypes.put("time", TIME);
            fooTypes.put("date", DATE);
            fooTypes.put("bar.field1", IntegerType.INTEGER);
            fooTypes.put("bar.field2", VarcharType.VARCHAR);
            fooTypes.put("bar.test.field4", DoubleType.DOUBLE);
            fooTypes.put("bar.test.field5", BooleanType.BOOLEAN);
            fooTypes.put("bar.test.field6", BigintType.BIGINT);
            fooTypes.put("bar.test.foobar.field1", IntegerType.INTEGER);
            fooTypes.put("bar.field3", RealType.REAL);
            fooTypes.put("bar.test2.field4", DoubleType.DOUBLE);
            fooTypes.put("bar.test2.field5", BooleanType.BOOLEAN);
            fooTypes.put("bar.test2.field6", BigintType.BIGINT);
            fooTypes.put("bar.test2.foobar.field1", IntegerType.INTEGER);
            // Enums currently map to VARCHAR
            fooTypes.put("field7", VarcharType.VARCHAR);

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

            fooFieldNames = new HashMap<>();
            fooPositionIndices = new HashMap<>();
            fooColumnHandles = new LinkedList<>();

            String[] fieldNames1 = {"field1"};
            Integer[] positionIndices1 = {0};
            fooFieldNames.put("field1", fieldNames1);
            fooPositionIndices.put("field1", positionIndices1);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "field1",
                    fooTypes.get("field1"),
                    false,
                    false,
                    fooFieldNames.get("field1"),
                    fooPositionIndices.get("field1"), null));


            String[] fieldNames2 = {"field2"};
            Integer[] positionIndices2 = {1};
            fooFieldNames.put("field2", fieldNames2);
            fooPositionIndices.put("field2", positionIndices2);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "field2",
                    fooTypes.get("field2"),
                    false,
                    false,
                    fieldNames2,
                    positionIndices2, null));

            String[] fieldNames3 = {"field3"};
            Integer[] positionIndices3 = {2};
            fooFieldNames.put("field3", fieldNames3);
            fooPositionIndices.put("field3", positionIndices3);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "field3",
                    fooTypes.get("field3"),
                    false,
                    false,
                    fieldNames3,
                    positionIndices3,  null));

            String[] fieldNames4 = {"field4"};
            Integer[] positionIndices4 = {3};
            fooFieldNames.put("field4", fieldNames4);
            fooPositionIndices.put("field4", positionIndices4);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "field4",
                    fooTypes.get("field4"),
                    false,
                    false,
                    fieldNames4,
                    positionIndices4, null));


            String[] fieldNames5 = {"field5"};
            Integer[] positionIndices5 = {4};
            fooFieldNames.put("field5", fieldNames5);
            fooPositionIndices.put("field5", positionIndices5);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "field5",
                    fooTypes.get("field5"),
                    false,
                    false,
                    fieldNames5,
                    positionIndices5, null));

            String[] fieldNames6 = {"field6"};
            Integer[] positionIndices6 = {5};
            fooFieldNames.put("field6", fieldNames6);
            fooPositionIndices.put("field6", positionIndices6);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "field6",
                    fooTypes.get("field6"),
                    false,
                    false,
                    fieldNames6,
                    positionIndices6, null));

            String[] fieldNames7 = {"timestamp"};
            Integer[] positionIndices7 = {6};
            fooFieldNames.put("timestamp", fieldNames7);
            fooPositionIndices.put("timestamp", positionIndices7);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "timestamp",
                    fooTypes.get("timestamp"),
                    false,
                    false,
                    fieldNames7,
                    positionIndices7, null));

            String[] fieldNames8 = {"time"};
            Integer[] positionIndices8 = {7};
            fooFieldNames.put("time", fieldNames8);
            fooPositionIndices.put("time", positionIndices8);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "time",
                    fooTypes.get("time"),
                    false,
                    false,
                    fieldNames8,
                    positionIndices8, null));

            String[] fieldNames9 = {"date"};
            Integer[] positionIndices9 = {8};
            fooFieldNames.put("date", fieldNames9);
            fooPositionIndices.put("date", positionIndices9);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "date",
                    fooTypes.get("date"),
                    false,
                    false,
                    fieldNames9,
                    positionIndices9, null));

            String[] bar_fieldNames1 = {"bar", "field1"};
            Integer[] bar_positionIndices1 = {9, 0};
            fooFieldNames.put("bar.field1", bar_fieldNames1);
            fooPositionIndices.put("bar.field1", bar_positionIndices1);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.field1",
                    fooTypes.get("bar.field1"),
                    false,
                    false,
                    bar_fieldNames1,
                    bar_positionIndices1, null));

            String[] bar_fieldNames2 = {"bar", "field2"};
            Integer[] bar_positionIndices2 = {9, 1};
            fooFieldNames.put("bar.field2", bar_fieldNames2);
            fooPositionIndices.put("bar.field2", bar_positionIndices2);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.field2",
                    fooTypes.get("bar.field2"),
                    false,
                    false,
                    bar_fieldNames2,
                    bar_positionIndices2, null));

            String[] bar_test_fieldNames4 = {"bar", "test", "field4"};
            Integer[] bar_test_positionIndices4 = {9, 2, 0};
            fooFieldNames.put("bar.test.field4", bar_test_fieldNames4);
            fooPositionIndices.put("bar.test.field4", bar_test_positionIndices4);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test.field4",
                    fooTypes.get("bar.test.field4"),
                    false,
                    false,
                    bar_test_fieldNames4,
                    bar_test_positionIndices4, null));

            String[] bar_test_fieldNames5 = {"bar", "test", "field5"};
            Integer[] bar_test_positionIndices5 = {9, 2, 1};
            fooFieldNames.put("bar.test.field5", bar_test_fieldNames5);
            fooPositionIndices.put("bar.test.field5", bar_test_positionIndices5);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test.field5",
                    fooTypes.get("bar.test.field5"),
                    false,
                    false,
                    bar_test_fieldNames5,
                    bar_test_positionIndices5, null));

            String[] bar_test_fieldNames6 = {"bar", "test", "field6"};
            Integer[] bar_test_positionIndices6 = {9, 2, 2};
            fooFieldNames.put("bar.test.field6", bar_test_fieldNames6);
            fooPositionIndices.put("bar.test.field6", bar_test_positionIndices6);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test.field6",
                    fooTypes.get("bar.test.field6"),
                    false,
                    false,
                    bar_test_fieldNames6,
                    bar_test_positionIndices6, null));

            String[] bar_test_foobar_fieldNames1 = {"bar", "test", "foobar", "field1"};
            Integer[] bar_test_foobar_positionIndices1 = {9, 2, 6, 0};
            fooFieldNames.put("bar.test.foobar.field1", bar_test_foobar_fieldNames1);
            fooPositionIndices.put("bar.test.foobar.field1", bar_test_foobar_positionIndices1);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test.foobar.field1",
                    fooTypes.get("bar.test.foobar.field1"),
                    false,
                    false,
                    bar_test_foobar_fieldNames1,
                    bar_test_foobar_positionIndices1, null));

            String[] bar_field3 = {"bar", "field3"};
            Integer[] bar_positionIndices3 = {9, 3};
            fooFieldNames.put("bar.field3", bar_field3);
            fooPositionIndices.put("bar.field3", bar_positionIndices3);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.field3",
                    fooTypes.get("bar.field3"),
                    false,
                    false,
                    bar_field3,
                    bar_positionIndices3, null));

            String[] bar_test2_fieldNames4 = {"bar", "test2", "field4"};
            Integer[] bar_test2_positionIndices4 = {9, 4, 0};
            fooFieldNames.put("bar.test2.field4", bar_test2_fieldNames4);
            fooPositionIndices.put("bar.test2.field4", bar_test2_positionIndices4);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test2.field4",
                    fooTypes.get("bar.test2.field4"),
                    false,
                    false,
                    bar_test2_fieldNames4,
                    bar_test2_positionIndices4, null));

            String[] bar_test2_fieldNames5 = {"bar", "test2", "field5"};
            Integer[] bar_test2_positionIndices5 = {9, 4, 1};
            fooFieldNames.put("bar.test2.field5", bar_test2_fieldNames5);
            fooPositionIndices.put("bar.test2.field5", bar_test2_positionIndices5);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test2.field5",
                    fooTypes.get("bar.test2.field5"),
                    false,
                    false,
                    bar_test2_fieldNames5,
                    bar_test2_positionIndices5, null));

            String[] bar_test2_fieldNames6 = {"bar", "test2", "field6"};
            Integer[] bar_test2_positionIndices6 = {9, 4, 2};
            fooFieldNames.put("bar.test2.field6", bar_test2_fieldNames6);
            fooPositionIndices.put("bar.test2.field6", bar_test2_positionIndices6);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test2.field6",
                    fooTypes.get("bar.test2.field6"),
                    false,
                    false,
                    bar_test2_fieldNames6,
                    bar_test2_positionIndices6, null));

            String[] bar_test2_foobar_fieldNames1 = {"bar", "test2", "foobar", "field1"};
            Integer[] bar_test2_foobar_positionIndices1 = {9, 4, 6, 0};
            fooFieldNames.put("bar.test2.foobar.field1", bar_test2_foobar_fieldNames1);
            fooPositionIndices.put("bar.test2.foobar.field1", bar_test2_foobar_positionIndices1);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "bar.test2.foobar.field1",
                    fooTypes.get("bar.test2.foobar.field1"),
                    false,
                    false,
                    bar_test2_foobar_fieldNames1,
                    bar_test2_foobar_positionIndices1, null));

            String[] fieldNames10 = {"field7"};
            Integer[] positionIndices10 = {10};
            fooFieldNames.put("field7", fieldNames10);
            fooPositionIndices.put("field7", positionIndices10);
            fooColumnHandles.add(new PulsarColumnHandle(pulsarConnectorId.toString(),
                    "field7",
                    fooTypes.get("field7"),
                    false,
                    false,
                    fieldNames10,
                    positionIndices10, null));

            fooColumnHandles.addAll(PulsarInternalColumn.getInternalFields().stream()
                .map(pulsarInternalColumn -> pulsarInternalColumn.getColumnHandle(pulsarConnectorId.toString(), false))
                .collect(Collectors.toList()));

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

            fooFunctions.put("bar.test.field4", integer -> integer + 1.0);
            fooFunctions.put("bar.test.field5", integer -> (integer + 1) % 2 == 0);
            fooFunctions.put("bar.test.field6", integer -> integer + 10L);
            fooFunctions.put("bar.test.foobar.field1", integer -> integer % 3);

            fooFunctions.put("bar.test2.field4", integer -> integer + 2.0);
            fooFunctions.put("bar.test2.field5", integer -> (integer + 1) % 32 == 0);
            fooFunctions.put("bar.test2.field6", integer -> integer + 15L);
            fooFunctions.put("bar.test2.foobar.field1", integer -> integer % 3);
            fooFunctions.put("field7", integer -> Foo.TestEnum.values()[integer % Foo.TestEnum.values().length]);

        } catch (Throwable e) {
            System.out.println("Error: " + e);
            System.out.println("Stacktrace: " + Arrays.asList(e.getStackTrace()));
        }
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

            PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
                    .setProducerName("test-producer").setSequenceId(i)
                    .setPublishTime(currentTimeMs + i).build();

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
                    throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
                }
                return ns;
            }
        });

        Topics topics = mock(Topics.class);
        when(topics.getList(anyString())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                String ns = (String) args[0];
                List<String> topics = getTopics(ns);
                if (topics.isEmpty()) {
                    throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
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
                    throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
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
                    throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
                }
            }
        });

        pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(tenants).when(pulsarAdmin).tenants();
        doReturn(namespaces).when(pulsarAdmin).namespaces();
        doReturn(topics).when(pulsarAdmin).topics();
        doReturn(schemas).when(pulsarAdmin).schemas();
        doReturn(pulsarAdmin).when(this.pulsarConnectorConfig).getPulsarAdmin();

        this.pulsarMetadata = new PulsarMetadata(pulsarConnectorId, this.pulsarConnectorConfig);
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
                        AsyncCallbacks.ReadEntriesCallback callback = (AsyncCallbacks.ReadEntriesCallback) args[1];
                        Object ctx = args[2];

                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                List < Entry > entries = new LinkedList<>();
                                for (int i = 0; i < readEntries; i++) {

                                    Foo.Bar foobar = new Foo.Bar();
                                    foobar.field1 = (int) fooFunctions.get("bar.test.foobar.field1").apply(count);

                                    Boo boo1 = new Boo();
                                    boo1.field4 = (double) fooFunctions.get("bar.test.field4").apply(count);
                                    boo1.field5 = (boolean) fooFunctions.get("bar.test.field5").apply(count);
                                    boo1.field6 = (long) fooFunctions.get("bar.test.field6").apply(count);
                                    boo1.foo = new Foo();
                                    boo1.boo = null;
                                    boo1.bar = new Bar();
                                    boo1.foobar = foobar;

                                    Boo boo2 = new Boo();
                                    boo2.field4 = (double) fooFunctions.get("bar.test2.field4").apply(count);
                                    boo2.field5 = (boolean) fooFunctions.get("bar.test2.field5").apply(count);
                                    boo2.field6 = (long) fooFunctions.get("bar.test2.field6").apply(count);
                                    boo2.foo = new Foo();
                                    boo2.boo = boo1;
                                    boo2.bar = new Bar();
                                    boo2.foobar = foobar;

                                    TestPulsarConnector.Bar bar = new TestPulsarConnector.Bar();
                                    bar.field1 = fooFunctions.get("bar.field1").apply(count) == null ? null : (int) fooFunctions.get("bar.field1").apply(count);
                                    bar.field2 = fooFunctions.get("bar.field2").apply(count) == null ? null : (String) fooFunctions.get("bar.field2").apply(count);
                                    bar.field3 = (float) fooFunctions.get("bar.field3").apply(count);
                                    bar.test = boo1;
                                    bar.test2 = count % 2 == 0 ? null : boo2;

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

                                    PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
                                            .setProducerName("test-producer").setSequenceId(positions.get(topic))
                                            .setPublishTime(System.currentTimeMillis()).build();

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
                }).when(readOnlyCursor).asyncReadEntries(anyInt(), any(), any());

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
                    fooColumnHandles, split.getValue(),
                    pulsarConnectorConfig, managedLedgerFactory, new ManagedLedgerConfig(),
                    new PulsarConnectorMetricsTracker(new NullStatsProvider())));
            this.pulsarRecordCursors.put(split.getKey(), pulsarRecordCursor);
        }
    }

    @AfterMethod
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
