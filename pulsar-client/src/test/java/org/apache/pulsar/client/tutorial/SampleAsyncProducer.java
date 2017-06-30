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
package org.apache.pulsar.client.tutorial;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SampleAsyncProducer {
    public static void main(String[] args) throws PulsarClientException, InterruptedException, IOException {
        PulsarClient pulsarClient = PulsarClient.create("http://127.0.0.1:8080");

        ProducerConfiguration conf = new ProducerConfiguration();
        conf.setSendTimeout(3, TimeUnit.SECONDS);
        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic", conf);

        List<CompletableFuture<MessageId>> futures = Lists.newArrayList();

        for (int i = 0; i < 10; i++) {
            final String content = "my-message-" + i;
            CompletableFuture<MessageId> future = producer.sendAsync(content.getBytes());

            future.handle((v, ex) -> {
                if (ex == null) {
                    log.info("Message persisted: {}", content);
                } else {
                    log.error("Error persisting message: {}", content, ex);
                }
                return null;
            });

            futures.add(future);
        }

        log.info("Waiting for async ops to complete");
        for (CompletableFuture<MessageId> future : futures) {
            future.join();
        }

        log.info("All operations completed");

        pulsarClient.close();
    }

    private static final Logger log = LoggerFactory.getLogger(SampleAsyncProducer.class);
}
