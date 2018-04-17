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
package org.apache.pulsar.replicator.api.kinesis;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Test for {@link KinesisReplicatorProducer}
 */
public class KinesisReplicatorProducerTest {

    @Test
    public void testKinesisProducer() throws Exception {
        String topicName = "test-topic";
        String streamName = "test-stream";
        Region region = Region.getRegion(Regions.US_WEST_2);
        AWSCredentials credentials = new AWSCredentials() {
            @Override
            public String getAWSAccessKeyId() {
                return "a1";
            }

            @Override
            public String getAWSSecretKey() {
                return "s1";
            }
        };
        KinesisReplicatorProducer kinesisProducer = new KinesisReplicatorProducer(topicName, streamName, region,
                credentials, null);

        Message<byte[]> message = MessageBuilder.create().setContent("".getBytes()).build();
        try {
            kinesisProducer.send(message).get(10, TimeUnit.MILLISECONDS);
            Assert.fail("should have fail due to invalid kinesis credential");
        } catch (Exception e) {// expected
        }

        kinesisProducer.close();
    }

}
