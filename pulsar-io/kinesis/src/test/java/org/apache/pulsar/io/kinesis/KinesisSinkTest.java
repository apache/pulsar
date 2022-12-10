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
package org.apache.pulsar.io.kinesis;

import java.util.concurrent.atomic.AtomicLong;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.awaitility.Awaitility;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class KinesisSinkTest {

    public static final String STREAM_NAME = "my-stream-1";
    public static LocalStackContainer LOCALSTACK_CONTAINER = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.0.4"))
            .withServices(LocalStackContainer.Service.KINESIS);

    @BeforeClass(alwaysRun = true)
    public void beforeClass() throws Exception {
        LOCALSTACK_CONTAINER.start();
        createClient().createStream(CreateStreamRequest.builder().streamName(STREAM_NAME).shardCount(1).build()).get();
    }

    @AfterClass(alwaysRun = true)
    public void afterClass() throws Exception {
        LOCALSTACK_CONTAINER.stop();
    }

    @Test
    public void testWrite() throws Exception {
        AtomicBoolean ackCalled = new AtomicBoolean();
        AtomicLong sequenceCounter = new AtomicLong(0);
        Message<GenericObject> message = mock(Message.class);
        when(message.getData()).thenReturn("hello".getBytes(StandardCharsets.UTF_8));
        final Record<GenericObject> pulsarRecord = new Record<GenericObject>() {

            @Override
            public Optional<String> getKey() {
                return Optional.of( "key-" + sequenceCounter.incrementAndGet());
            }

            @Override
            public GenericObject getValue() {
                // Value comes from the message raw data, not the GenericObject
                return null;
            }

            @Override
            public void ack() {
                ackCalled.set(true);
            }

            @Override
            public Optional<Message<GenericObject>> getMessage() {
                return Optional.of(message);
            }
        };

        try (final KinesisSink sink = new KinesisSink()) {
            Map<String, Object> map = createConfig();
            SinkContext mockSinkContext = mock(SinkContext.class);

            sink.open(map, mockSinkContext);
            for (int i = 0; i < 10; i++) {
                sink.write(pulsarRecord);
            }
            Awaitility.await().untilAsserted(() -> {
                assertTrue(ackCalled.get());
            });
            final GetRecordsResponse getRecords = getStreamRecords();
            assertEquals(getRecords.records().size(), 10);

            for (software.amazon.awssdk.services.kinesis.model.Record record : getRecords.records()) {
                assertEquals(record.data().asString(StandardCharsets.UTF_8), "hello");
            }
        }
    }

    private Map<String, Object> createConfig() {
        final URI endpointOverride = LOCALSTACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.KINESIS);
        Map<String, Object> map = new HashMap<>();
        map.put("awsEndpoint", endpointOverride.getHost());
        map.put("awsEndpointPort", endpointOverride.getPort());
        map.put("skipCertificateValidation", true);
        map.put("awsKinesisStreamName", STREAM_NAME);
        map.put("awsRegion", "us-east-1");
        map.put("awsCredentialPluginParam", "{\"accessKey\":\"access\",\"secretKey\":\"secret\"}");
        return map;
    }

    private KinesisAsyncClient createClient() {
        final KinesisAsyncClient client = KinesisAsyncClient.builder()
                .credentialsProvider(new AwsCredentialsProvider() {
                    @Override
                    public AwsCredentials resolveCredentials() {
                        return AwsBasicCredentials.create(
                                "access",
                                "secret");
                    }
                })
                .region(Region.US_EAST_1)
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(LocalStackContainer.Service.KINESIS))
                .build();
        return client;
    }

    @SneakyThrows
    private GetRecordsResponse getStreamRecords() {
        final KinesisAsyncClient client = createClient();
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
        final GetRecordsResponse response = client.getRecords(
                        GetRecordsRequest
                                .builder()
                                .shardIterator(iterator)
                                .build())
                .get();
        return response;
    }

}
