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
package org.apache.pulsar.tests.integration.io.sinks;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
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
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.testng.Assert.assertTrue;

@Slf4j
public class KinesisSinkTester extends SinkTester<LocalStackContainer> {

    private static final String NAME = "kinesis";
    public static final String STREAM_NAME = "my-stream-1";
    private KinesisAsyncClient client;

    public KinesisSinkTester() {
        super(NAME, SinkType.KINESIS);

        sinkConfig.put("awsKinesisStreamName", STREAM_NAME);
        sinkConfig.put("awsRegion", "us-east-1");
        sinkConfig.put("awsCredentialPluginParam", "{\"accessKey\":\"access\",\"secretKey\":\"secret\"}");
    }


    @Override
    public void prepareSink() throws Exception {
        final LocalStackContainer localStackContainer = getServiceContainer();
        final URI endpointOverride = localStackContainer.getEndpointOverride(LocalStackContainer.Service.KINESIS);
        sinkConfig.put("awsEndpoint", NAME);
        sinkConfig.put("awsEndpointPort", endpointOverride.getPort());
        sinkConfig.put("skipCertificateValidation", true);
        client = KinesisAsyncClient.builder().credentialsProvider(new AwsCredentialsProvider() {
                    @Override
                    public AwsCredentials resolveCredentials() {
                        return AwsBasicCredentials.create(
                                "access",
                                "secret");
                    }
                })
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
    protected LocalStackContainer createSinkService(PulsarCluster cluster) {
        return new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                .withServices(LocalStackContainer.Service.KINESIS);
    }

    @Override
    @SneakyThrows
    public void validateSinkResult(Map<String, String> kvs) {
        Awaitility.await().untilAsserted(() -> validateSinkResult());
    }

    @SneakyThrows
    private void validateSinkResult() {
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
        assertTrue(response.hasRecords());
        for (Record record : response.records()) {
            assertTrue(record.data().asString(StandardCharsets.UTF_8).startsWith("value-"));
        }
    }
}
