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
package org.apache.pulsar.broker;

import static org.testng.Assert.assertEquals;
import java.io.File;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class EndToEndMetadataTest extends BaseMetadataStoreTest {

    private File tempDir;

    @BeforeClass(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        super.setup();
        tempDir = IOUtils.createTempDir("bookies", "test");
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
        FileUtils.deleteDirectory(tempDir);
    }

    @Test(dataProvider = "impl")
    public void testPublishConsume(String provider, Supplier<String> urlSupplier) throws Exception {

        @Cleanup
        EmbeddedPulsarCluster epc = EmbeddedPulsarCluster.builder()
                .numBrokers(1)
                .numBookies(1)
                .metadataStoreUrl(urlSupplier.get())
                .clearOldData(true)
                .build();

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(epc.getServiceUrl())
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic("my-topic")
                .create();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("my-topic")
                .subscriptionName("my-sub")
                .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.sendAsync("hello-" + i);
        }

        producer.flush();

        for (int i = 0; i < 10; i++) {
            Message<String> msg = consumer.receive();
            assertEquals(msg.getValue(), "hello-" + i);
            consumer.acknowledge(msg);
        }
    }

}
