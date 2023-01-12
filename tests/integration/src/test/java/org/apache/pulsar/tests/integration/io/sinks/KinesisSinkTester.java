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
package org.apache.pulsar.tests.integration.io.sinks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

@Slf4j
public class KinesisSinkTester extends SinkTester<LocalStackContainer> {

    private static final String NAME = "kinesis";
    private static final int LOCALSTACK_SERVICE_PORT = 4566;
    public static final String STREAM_NAME = "my-stream-1";
    public static final ObjectReader READER = ObjectMapperFactory.getMapper().reader();
    private final boolean withSchema;
    private KinesisAsyncClient client;

    public KinesisSinkTester(boolean withSchema) {
        super(NAME, SinkType.KINESIS);
        this.withSchema = withSchema;

        sinkConfig.put("awsKinesisStreamName", STREAM_NAME);
        sinkConfig.put("awsRegion", "us-east-1");
        sinkConfig.put("awsCredentialPluginParam", "{\"accessKey\":\"access\",\"secretKey\":\"secret\"}");
        if (withSchema) {
            sinkConfig.put("messageFormat", "FULL_MESSAGE_IN_JSON_EXPAND_VALUE");
        }
    }

    @Override
    public Schema<?> getInputTopicSchema() {
        if (withSchema) {
            // we do not want to enforce a Schema
            // at the beginning of the test
            return Schema.AUTO_CONSUME();
        } else {
            return Schema.STRING;
        }
    }


    @Override
    public void prepareSink() throws Exception {
        final LocalStackContainer localStackContainer = getServiceContainer();
        final URI endpointOverride = localStackContainer.getEndpointOverride(LocalStackContainer.Service.KINESIS);
        sinkConfig.put("awsEndpoint", NAME);
        sinkConfig.put("awsEndpointPort", LOCALSTACK_SERVICE_PORT);
        sinkConfig.put("skipCertificateValidation", true);
        client = KinesisAsyncClient.builder().credentialsProvider(() -> AwsBasicCredentials.create(
                "access",
                "secret"))
                .region(Region.US_EAST_1)
                .endpointOverride(endpointOverride)
                .build();
        log.info("prepareSink for kinesis: creating stream {}, endpoint {}", STREAM_NAME, endpointOverride);
        client.createStream(CreateStreamRequest.builder()
                .streamName(STREAM_NAME)
                .shardCount(1)
                .build())
                .get();
        log.info("prepareSink for kinesis: created stream {}", STREAM_NAME);
    }

    @Override
    public void stopServiceContainer() {
        if (client != null) {
            client.close();
        }
        super.stopServiceContainer();
    }

    @Override
    protected LocalStackContainer createSinkService(PulsarCluster cluster) {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.0.4"))
                .withServices(LocalStackContainer.Service.KINESIS);
    }

    @Override
    public void produceMessage(int numMessages, PulsarClient client,
                               String inputTopicName, LinkedHashMap<String, String> kvs) throws Exception {
        if (withSchema) {
            Schema<KeyValue<SimplePojo, SimplePojo>> kvSchema =
                    Schema.KeyValue(Schema.JSON(SimplePojo.class),
                            Schema.AVRO(SimplePojo.class), KeyValueEncodingType.SEPARATED);

            @Cleanup
            Producer<KeyValue<SimplePojo, SimplePojo>> producer = client.newProducer(kvSchema)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                String key = String.valueOf(i);
                kvs.put(key, key);
                final SimplePojo keyPojo = new SimplePojo(
                        "f1_" + i,
                        "f2_" + i,
                        Arrays.asList(i, i +1),
                        new HashSet<>(Arrays.asList((long) i)),
                        ImmutableMap.of("map1_k_" + i, "map1_kv_" + i));
                final SimplePojo valuePojo = new SimplePojo(
                        String.valueOf(i),
                        "v2_" + i,
                        Arrays.asList(i, i +1),
                        new HashSet<>(Arrays.asList((long) i)),
                        ImmutableMap.of("map1_v_" + i, "map1_vv_" + i));
                producer.newMessage()
                        .value(new KeyValue<>(keyPojo, valuePojo))
                        .send();
            }
        } else {
            @Cleanup
            Producer<String> producer = client.newProducer(Schema.STRING)
                    .topic(inputTopicName)
                    .create();

            for (int i = 0; i < numMessages; i++) {
                String key = "key-" + i;
                String value = "value-" + i;
                kvs.put(key, value);
                producer.newMessage()
                        .key(key)
                        .value(value)
                        .send();
            }
        }
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        Awaitility.await().untilAsserted(() -> internalValidateSinkResult(kvs));
    }

    @SneakyThrows
    private void internalValidateSinkResult(Map<String, String> kvs) {
        final String shardId = client.listShards(
                ListShardsRequest.builder()
                        .streamName(STREAM_NAME)
                        .build()
        ).get()
                .shards()
                .get(0)
                .shardId();

        final String iterator = client.getShardIterator(GetShardIteratorRequest.builder()
                .streamName(STREAM_NAME)
                .shardId(shardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .build())
                .get()
                .shardIterator();

        Map<String, String> actualKvs = new LinkedHashMap<>();

        addMoreRecords(actualKvs, iterator);

        assertEquals(actualKvs, kvs);
    }

    @SneakyThrows
    private void parseRecordData(Map<String, String> actualKvs, String data, String partitionKey) {
        if (withSchema) {
            JsonNode payload = READER.readTree(data).at("/payload");
            String i = payload.at("/value/field1").asText();
            assertEquals(payload.at("/value/field2").asText(), "v2_" + i);
            assertEquals(payload.at("/key/field1").asText(), "f1_" + i);
            assertEquals(payload.at("/key/field2").asText(), "f2_" + i);
            actualKvs.put(i, i);
        } else {
            actualKvs.put(partitionKey, data);
        }
    }

    @SneakyThrows
    private void addMoreRecords(Map<String, String> actualKvs, String iterator) {
        GetRecordsResponse response;
        List<KinesisClientRecord> aggRecords = new ArrayList<>();
        do {
            GetRecordsRequest request = GetRecordsRequest.builder().shardIterator(iterator).build();
            response = client.getRecords(request).get();
            if (response.hasRecords()) {
                for (Record record : response.records()) {
                    // KinesisSink uses KPL with aggregation enabled (by default).
                    // However, due to the async state initialization of the KPL internal ShardMap,
                    // the first sinked records might not be aggregated in Kinesis.
                    // ref: https://github.com/awslabs/amazon-kinesis-producer/issues/131
                    try {
                        String data = record.data().asString(StandardCharsets.UTF_8);
                        parseRecordData(actualKvs, data, record.partitionKey());
                    } catch (UncheckedIOException e) {
                        aggRecords.add(KinesisClientRecord.fromRecord(record));
                    }
                }
            }
            iterator = response.nextShardIterator();
            // millisBehindLatest equals zero when record processing is caught up,
            // and there are no new records to process at this moment.
            // See https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html#Streams-GetRecords-response-MillisBehindLatest
        } while (response.millisBehindLatest() != 0);

        for (KinesisClientRecord record : new AggregatorUtil().deaggregate(aggRecords)) {
            String data = new String(record.data().array(), StandardCharsets.UTF_8);
            parseRecordData(actualKvs, data, record.partitionKey());
        }
    }

    @Data
    @AllArgsConstructor
    public static final class SimplePojo {
        private String field1;
        private String field2;
        private List<Integer> list1;
        private Set<Long> set1;
        private Map<String, String> map1;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
