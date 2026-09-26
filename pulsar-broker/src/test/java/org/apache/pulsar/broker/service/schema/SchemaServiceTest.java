/*
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
package org.apache.pulsar.broker.service.schema;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy.BACKWARD;
import static org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.opentelemetry.api.common.Attributes;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.NotExistSchemaException;
import org.apache.pulsar.broker.service.schema.generated.enums.NativeLegacyEnum;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SchemaServiceTest extends MockedPulsarServiceBaseTest {

    private static final Clock MockClock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

    private final String schemaId1 = "1/2/3/4";
    private static final String userId = "user";

    private static final String schemaJson1 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema"
                    + ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData1 = getSchemaData(schemaJson1);

    private static final String schemaJson2 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema"
                    + ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},"
                    + "{\"name\":\"field2\",\"type\":\"string\",\"default\":\"foo\"}]}";
    private static final SchemaData schemaData2 = getSchemaData(schemaJson2);

    private static final String schemaJson3 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema"
                    + ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},"
                    + "{\"name\":\"field2\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData3 = getSchemaData(schemaJson3);

    private SchemaRegistryServiceImpl schemaRegistryService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSchemaRegistryStorageClassName(
                "org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory");
        super.internalSetup();
        BookkeeperSchemaStorage storage = new BookkeeperSchemaStorage(pulsar);
        storage.start();
        Map<SchemaType, SchemaCompatibilityCheck> checkMap = new HashMap<>();
        checkMap.put(SchemaType.AVRO, new AvroSchemaCompatibilityCheck());
        checkMap.put(SchemaType.PROTOBUF_NATIVE, new ProtobufNativeSchemaAdvancedCompatibilityCheck());
        schemaRegistryService = new SchemaRegistryServiceImpl(storage, checkMap, MockClock, pulsar);

        var schemaRegistryStats =
                Mockito.spy((SchemaRegistryStats) FieldUtils.readField(schemaRegistryService, "stats", true));
        // Disable periodic cleanup of Prometheus entries.
        Mockito.doNothing().when(schemaRegistryStats).run();
        FieldUtils.writeField(schemaRegistryService, "stats", schemaRegistryStats, true);

        setupDefaultTenantAndNamespace();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        schemaRegistryService.close();
    }

    @Test
    public void testSchemaRegistryMetrics() throws Exception {
        String schemaId = "tenant/ns/topic" + UUID.randomUUID();
        String namespace = TopicName.get(schemaId).getNamespace();
        putSchema(schemaId, schemaData1, version(0));
        getSchema(schemaId, version(0));
        deleteSchema(schemaId, version(1));

        var otelMetrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertThat(otelMetrics).anySatisfy(metric -> assertThat(metric)
                .hasName(SchemaRegistryStats.SCHEMA_REGISTRY_REQUEST_DURATION_METRIC_NAME)
                .hasHistogramSatisfying(histogram -> histogram.hasPointsSatisfying(
                        point -> point
                                .hasAttributes(Attributes.of(OpenTelemetryAttributes.PULSAR_NAMESPACE, "tenant/ns",
                                        SchemaRegistryStats.REQUEST_TYPE_KEY, "delete",
                                        SchemaRegistryStats.RESPONSE_TYPE_KEY, "success"))
                                .hasCount(1),
                        point -> point
                                .hasAttributes(Attributes.of(OpenTelemetryAttributes.PULSAR_NAMESPACE, "tenant/ns",
                                        SchemaRegistryStats.REQUEST_TYPE_KEY, "put",
                                        SchemaRegistryStats.RESPONSE_TYPE_KEY, "success"))
                                .hasCount(1),
                        point -> point
                                .hasAttributes(Attributes.of(OpenTelemetryAttributes.PULSAR_NAMESPACE, "tenant/ns",
                                        SchemaRegistryStats.REQUEST_TYPE_KEY, "list",
                                        SchemaRegistryStats.RESPONSE_TYPE_KEY, "success"))
                                .hasCount(1),
                        point -> point
                                .hasAttributes(Attributes.of(OpenTelemetryAttributes.PULSAR_NAMESPACE, "tenant/ns",
                                        SchemaRegistryStats.REQUEST_TYPE_KEY, "get",
                                        SchemaRegistryStats.RESPONSE_TYPE_KEY, "success"))
                                .hasCount(1)
                )));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, false, false, false, output);
        output.flush();
        String metricsStr = output.toString(StandardCharsets.UTF_8);
        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        // The *_ops_failed_total counters are registered on the default Prometheus
        // registry (a JVM-wide static), so labels accumulated by other tests in the
        // same JVM persist here. Only assert that no failed metric exists for THIS
        // test's namespace.
        Collection<Metric> delMetrics = metrics.get("pulsar_schema_del_ops_failed_total");
        assertThat(delMetrics).noneMatch(metric -> namespace.equals(metric.tags.get("namespace")));
        Collection<Metric> getMetrics = metrics.get("pulsar_schema_get_ops_failed_total");
        assertThat(getMetrics).noneMatch(metric -> namespace.equals(metric.tags.get("namespace")));
        Collection<Metric> putMetrics = metrics.get("pulsar_schema_put_ops_failed_total");
        assertThat(putMetrics).noneMatch(metric -> namespace.equals(metric.tags.get("namespace")));

        Collection<Metric> deleteLatency = metrics.get("pulsar_schema_del_ops_latency_count");
        assertThat(deleteLatency).anySatisfy(metric -> {
            Assert.assertEquals(metric.tags.get("namespace"), namespace);
            Assert.assertTrue(metric.value > 0);
        });

        Collection<Metric> getLatency = metrics.get("pulsar_schema_get_ops_latency_count");
        assertThat(getLatency).anySatisfy(metric -> {
            Assert.assertEquals(metric.tags.get("namespace"), namespace);
            Assert.assertTrue(metric.value > 0);
        });

        Collection<Metric> putLatency = metrics.get("pulsar_schema_put_ops_latency_count");
        assertThat(putLatency).anySatisfy(metric -> {
            Assert.assertEquals(metric.tags.get("namespace"), namespace);
            Assert.assertTrue(metric.value > 0);
        });
    }

    @Test
    public void writeReadBackDeleteSchemaEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));

        SchemaData latest = getLatestSchema(schemaId1, version(0));
        assertEquals(schemaData1, latest);

        deleteSchema(schemaId1, version(1));

        assertNull(schemaRegistryService.getSchema(schemaId1).get());
    }

    @Test
    public void findSchemaVersionTest() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        assertEquals(0, schemaRegistryService.findSchemaVersion(schemaId1, schemaData1).get().longValue());
    }

    @Test
    public void deleteSchemaAndAddSchema() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        SchemaData latest = getLatestSchema(schemaId1, version(0));
        assertEquals(schemaData1, latest);

        deleteSchema(schemaId1, version(1));

        assertNull(schemaRegistryService.getSchema(schemaId1).get());

        putSchema(schemaId1, schemaData1, version(2));

        latest = getLatestSchema(schemaId1, version(2));
        assertEquals(schemaData1, latest);

    }

    @Test
    public void getReturnsTheLastWrittenEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData latest = getLatestSchema(schemaId1, version(1));
        assertEquals(schemaData2, latest);

    }

    @Test
    public void getByVersionReturnsTheCorrectEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData version0 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version0);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry2() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData version1 = getSchema(schemaId1, version(1));
        assertEquals(schemaData2, version1);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry3() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));

        SchemaData version1 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version1);
    }

    @Test
    public void getAllVersionSchema() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));

        List<SchemaData> allSchemas = getAllSchemas(schemaId1);
        assertEquals(schemaData1, allSchemas.get(0));
        assertEquals(schemaData2, allSchemas.get(1));
        assertEquals(schemaData3, allSchemas.get(2));
    }

    @Test
    public void addLotsOfEntriesThenDelete() throws Exception {

        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));

        SchemaData version0 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version0);

        SchemaData version1 = getSchema(schemaId1, version(1));
        assertEquals(schemaData2, version1);

        SchemaData version2 = getSchema(schemaId1, version(2));
        assertEquals(schemaData3, version2);

        deleteSchema(schemaId1, version(3));

        SchemaRegistry.SchemaAndMetadata version3 = schemaRegistryService.getSchema(schemaId1, version(3)).get();
        assertNull(version3);

    }

    @Test
    public void writeSchemasToDifferentIds() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        String schemaId2 = "id2";
        putSchema(schemaId2, schemaData3, version(0));

        SchemaData withFirstId = getLatestSchema(schemaId1, version(0));
        SchemaData withDifferentId = getLatestSchema(schemaId2, version(0));

        assertEquals(schemaData1, withFirstId);
        assertEquals(schemaData3, withDifferentId);
    }

    @Test
    public void dontReAddExistingSchemaAtRoot() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData1, version(0));
    }

    @Test
    public void trimDeletedSchemaAndGetListTest() throws Exception {
        List<SchemaAndMetadata> list = new ArrayList<>();
        CompletableFuture<SchemaVersion> put = schemaRegistryService.putSchemaIfAbsent(
                schemaId1, schemaData1, SchemaCompatibilityStrategy.FULL);
        SchemaVersion newVersion = put.get();
        list.add(new SchemaAndMetadata(schemaId1, schemaData1, newVersion));
        put = schemaRegistryService.putSchemaIfAbsent(
                schemaId1, schemaData2, SchemaCompatibilityStrategy.FULL);
        newVersion = put.get();
        list.add(new SchemaAndMetadata(schemaId1, schemaData2, newVersion));
        List<SchemaAndMetadata> list1 = schemaRegistryService.trimDeletedSchemaAndGetList(schemaId1).get();
        assertEquals(list.size(), list1.size());
        HashFunction hashFunction = Hashing.sha256();
        for (int i = 0; i < list.size(); i++) {
            SchemaAndMetadata schemaAndMetadata1 = list.get(i);
            SchemaAndMetadata schemaAndMetadata2 = list1.get(i);
            assertEquals(hashFunction.hashBytes(schemaAndMetadata1.schema.getData()).asBytes(),
                    hashFunction.hashBytes(schemaAndMetadata2.schema.getData()).asBytes());
            assertEquals(((LongSchemaVersion) schemaAndMetadata1.version).getVersion()
                    , ((LongSchemaVersion) schemaAndMetadata2.version).getVersion());
            assertEquals(schemaAndMetadata1.id, schemaAndMetadata2.id);
        }
    }

    @Test
    public void dontReAddExistingSchemaInMiddle() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));
        putSchema(schemaId1, schemaData2, version(1));
    }

    @Test
    public void checkIsCompatible() throws Exception {
        var schemaId = BrokerTestUtil.newUniqueName("tenant/ns/topic");
        putSchema(schemaId, schemaData1, version(0), BACKWARD_TRANSITIVE);
        putSchema(schemaId, schemaData2, version(1), BACKWARD_TRANSITIVE);

        var timeout = Duration.ofSeconds(1);
        assertThat(schemaRegistryService.isCompatible(schemaId, schemaData3, BACKWARD))
                .succeedsWithin(timeout, InstanceOfAssertFactories.BOOLEAN)
                .isTrue();
        assertThat(schemaRegistryService.isCompatible(schemaId, schemaData3, BACKWARD_TRANSITIVE))
                .failsWithin(timeout)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(IncompatibleSchemaException.class);
        assertThatThrownBy(() -> putSchema(schemaId, schemaData3, version(2), BACKWARD_TRANSITIVE))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IncompatibleSchemaException.class);

        assertThat(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                        .hasName(SchemaRegistryStats.COMPATIBLE_COUNTER_METRIC_NAME)
                        .hasLongSumSatisfying(
                                sum -> sum.hasPointsSatisfying(
                                    point -> point
                                            .hasAttributes(Attributes.of(
                                                    OpenTelemetryAttributes.PULSAR_NAMESPACE, "tenant/ns",
                                                    SchemaRegistryStats.COMPATIBILITY_CHECK_RESPONSE_KEY,
                                                    "compatible"))
                                            .hasValue(2),
                                    point -> point
                                            .hasAttributes(Attributes.of(
                                                    OpenTelemetryAttributes.PULSAR_NAMESPACE, "tenant/ns",
                                                    SchemaRegistryStats.COMPATIBILITY_CHECK_RESPONSE_KEY,
                                                    "incompatible"))
                                            .hasValue(2))));
    }

    @Test
    public void testAdvancedNativeHistoryAndReuse() throws Exception {
        String id = BrokerTestUtil.newUniqueName("tenant/ns/native-topic");
        SchemaData v1 = nativeSchema("id", 1, FieldDescriptorProto.Type.TYPE_INT32, false);
        SchemaData v2 = nativeSchema(null, 0, null, false);
        SchemaData v3 = nativeSchema("id", 1, FieldDescriptorProto.Type.TYPE_STRING, false);
        putSchema(id, v1, version(0), BACKWARD_TRANSITIVE);
        putSchema(id, v2, version(1), BACKWARD_TRANSITIVE);
        putSchema(id, v2, version(1), BACKWARD_TRANSITIVE);
        putSchema(id, v3, version(2), BACKWARD);

        assertThat(schemaRegistryService.isCompatible(id, v3, BACKWARD)).succeedsWithin(Duration.ofSeconds(2),
                InstanceOfAssertFactories.BOOLEAN).isTrue();
        assertThat(schemaRegistryService.isCompatible(id, v3, BACKWARD_TRANSITIVE))
                .failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(IncompatibleSchemaException.class);
    }

    @Test
    public void testAdvancedNativeConsumerStrategyMapping() throws Exception {
        String id = BrokerTestUtil.newUniqueName("tenant/ns/native-consumer");
        SchemaData required = nativeSchema("id", 1, FieldDescriptorProto.Type.TYPE_INT32, true);
        SchemaData optional = nativeSchema("id", 1, FieldDescriptorProto.Type.TYPE_INT32, false);
        putSchema(id, required, version(0));
        for (SchemaCompatibilityStrategy strategy : List.of(SchemaCompatibilityStrategy.BACKWARD,
                SchemaCompatibilityStrategy.FORWARD, SchemaCompatibilityStrategy.FULL,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE,
                SchemaCompatibilityStrategy.FORWARD_TRANSITIVE)) {
            schemaRegistryService.checkConsumerCompatibility(id, optional, strategy).get();
        }
        assertThat(schemaRegistryService.checkConsumerCompatibility(id, optional,
                SchemaCompatibilityStrategy.FULL_TRANSITIVE))
                .failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(IncompatibleSchemaException.class);
        schemaRegistryService.checkConsumerCompatibility(id, optional,
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE).get();
    }

    @Test
    public void testAdvancedNativeConsumerLatestOnlyAndMissingSchema() throws Exception {
        String id = BrokerTestUtil.newUniqueName("tenant/ns/native-consumer-latest");
        SchemaData v1 = nativeSchema("id", 1, FieldDescriptorProto.Type.TYPE_INT32, false);
        SchemaData v2 = nativeSchema(null, 0, null, false);
        SchemaData consumer = nativeSchema("id", 1, FieldDescriptorProto.Type.TYPE_STRING, false);
        putSchema(id, v1, version(0), BACKWARD);
        putSchema(id, v2, version(1), BACKWARD);
        schemaRegistryService.checkConsumerCompatibility(id, consumer,
                SchemaCompatibilityStrategy.FORWARD_TRANSITIVE).get();
        assertThat(schemaRegistryService.checkConsumerCompatibility(id, consumer,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE))
                .failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(IncompatibleSchemaException.class);

        String absent = BrokerTestUtil.newUniqueName("tenant/ns/native-missing");
        schemaRegistryService.checkConsumerCompatibility(absent, consumer,
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE).get();
        assertThat(schemaRegistryService.checkConsumerCompatibility(absent, consumer,
                SchemaCompatibilityStrategy.BACKWARD))
                .failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(NotExistSchemaException.class);
    }

    @Test
    public void testAdvancedNativeUnsupportedHistoryEqualityShortcuts() throws Exception {
        String id = BrokerTestUtil.newUniqueName("tenant/ns/native-legacy-enum");
        SchemaData unsupported = SchemaData.builder().type(SchemaType.PROTOBUF_NATIVE)
                .data(ProtobufNativeSchemaUtils.serialize(NativeLegacyEnum.Order.getDescriptor()))
                .user(userId).timestamp(MockClock.millis()).build();
        putSchema(id, unsupported, version(0), BACKWARD);
        putSchema(id, unsupported, version(0), BACKWARD);
        schemaRegistryService.checkConsumerCompatibility(id, unsupported, SchemaCompatibilityStrategy.BACKWARD).get();
        assertThat(schemaRegistryService.checkConsumerCompatibility(id, unsupported,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE))
                .failsWithin(Duration.ofSeconds(2))
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(IncompatibleSchemaException.class);
    }

    private static SchemaData nativeSchema(String fieldName, int number, FieldDescriptorProto.Type type,
                                           boolean required) throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Order");
        if (fieldName != null) {
            message.addField(FieldDescriptorProto.newBuilder().setName(fieldName).setNumber(number).setType(type)
                    .setLabel(required ? FieldDescriptorProto.Label.LABEL_REQUIRED
                            : FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }
        FileDescriptorProto file = FileDescriptorProto.newBuilder().setName("native-order.proto")
                .setPackage("example").setSyntax("proto2").addMessageType(message).build();
        return SchemaData.builder().type(SchemaType.PROTOBUF_NATIVE)
                .data(ProtobufNativeSchemaUtils.serialize(FileDescriptor.buildFrom(file, new FileDescriptor[0])
                        .findMessageTypeByName("Order"))).user(userId).timestamp(MockClock.millis()).build();
    }

    @Test
    public void testSchemaStorageFailed() throws Exception {
        conf.setSchemaRegistryStorageClassName("Unknown class name");
        try {
            restartBroker();
            fail("An exception should have been thrown");
        } catch (Exception rte) {
            Throwable e = rte.getCause();
            Assert.assertEquals(e.getClass(), PulsarServerException.class);
            Assert.assertTrue(e.getMessage().contains("User class must be in class path"));
        }
    }

    @Test
    public void testBrokerRejectsBothNativeCheckersAtStartup() throws Exception {
        conf.setSchemaRegistryCompatibilityCheckers(Set.of(
                ProtobufNativeSchemaCompatibilityCheck.class.getName(),
                ProtobufNativeSchemaAdvancedCompatibilityCheck.class.getName()));
        assertThatThrownBy(this::restartBroker)
                .hasRootCauseMessage("Configure only one PROTOBUF_NATIVE compatibility checker");
    }

    private void putSchema(String schemaId, SchemaData schema, SchemaVersion expectedVersion) throws Exception {
        putSchema(schemaId, schema, expectedVersion, SchemaCompatibilityStrategy.FULL);
    }

    private void putSchema(String schemaId, SchemaData schema, SchemaVersion expectedVersion,
                           SchemaCompatibilityStrategy strategy) throws ExecutionException, InterruptedException {
        CompletableFuture<SchemaVersion> put = schemaRegistryService.putSchemaIfAbsent(
                schemaId, schema, strategy);
        SchemaVersion newVersion = put.get();
        assertEquals(expectedVersion, newVersion);
    }

    private SchemaData getLatestSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId).get();
        assertEquals(expectedVersion, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private SchemaData getSchema(String schemaId, SchemaVersion version) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId, version).get();
        assertEquals(version, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private List<SchemaData> getAllSchemas(String schemaId) throws Exception {
        List<SchemaData> result = new ArrayList<>();
        for (CompletableFuture<SchemaRegistry.SchemaAndMetadata> schema :
                schemaRegistryService.getAllSchemas(schemaId).get()) {
            result.add(schema.get().schema);
        }
        return result;
    }

    private void deleteSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaVersion version = schemaRegistryService.deleteSchema(schemaId, userId, false).get();
        assertEquals(expectedVersion, version);
    }

    private static SchemaData getSchemaData(String schemaJson) {
        return SchemaData.builder()
                .data(schemaJson.getBytes())
                .type(SchemaType.AVRO)
                .user(userId)
                .timestamp(MockClock.millis())
                .build();
    }

    private SchemaVersion version(long version) {
        return new LongSchemaVersion(version);
    }

    @Test
    public void testKeyValueSchema() throws Exception {
        final String topicName = "persistent://public/default/testKeyValueSchema";
        admin.topics().createNonPartitionedTopic(BrokerTestUtil.newUniqueName(topicName));

        final SchemaInfo schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
                "keyValue",
                SchemaInfo.builder().type(SchemaType.STRING).schema(new byte[0])
                        .build(),
                SchemaInfo.builder().type(SchemaType.BOOLEAN).schema(new byte[0])
                        .build(), KeyValueEncodingType.SEPARATED);
        Assert.assertTrue(admin.schemas().testCompatibility(topicName, schemaInfo).isCompatibility());
        admin.schemas().createSchema(topicName, schemaInfo);

        final IsCompatibilityResponse isCompatibilityResponse = admin.schemas()
                .testCompatibility(topicName, schemaInfo);
        Assert.assertTrue(isCompatibilityResponse.isCompatibility());

        final SchemaInfoWithVersion schemaInfoWithVersion = admin.schemas().getSchemaInfoWithVersion(topicName);
        Assert.assertEquals(schemaInfoWithVersion.getVersion(), 0);

        final Long version1 = admin.schemas().getVersionBySchema(topicName, schemaInfo);
        Assert.assertEquals(version1, 0);

        final Long version2 = admin.schemas().getVersionBySchema(topicName, schemaInfoWithVersion.getSchemaInfo());
        Assert.assertEquals(version2, 0);

    }
}
