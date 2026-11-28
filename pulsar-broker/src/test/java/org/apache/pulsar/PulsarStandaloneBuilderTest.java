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

package org.apache.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarStandaloneBuilderTest {
    @Test
    public void testStartFromJava() throws Exception {
        PulsarStandalone standalone = PulsarStandaloneBuilder.instance()
                .withTempDirectory()
                .build();
        Path tempBaseDir = standalone.getTempBaseDir();
        try {
            standalone.getConfig().setBrokerServicePort(Optional.of(0));
            standalone.getConfig().setWebServicePort(Optional.of(0));
            standalone.start();
            try (PulsarAdmin admin = standalone.buildAdmin()) {
                assertThat(admin.namespaces().getNamespaces("public")).contains("public/default");
            }
            assertThat(tempBaseDir).exists();
            assertThat(standalone.getConfig().getMetadataStoreUrl())
                    .contains(tempBaseDir.toAbsolutePath().toString());
            try (PulsarClient client = standalone.buildClient()) {
                Producer<String> producer = client.newProducer(Schema.STRING)
                        .topic("test-topic").create();
                Consumer<String> consumer = client.newConsumer(Schema.STRING)
                        .topic("test-topic").subscriptionName("sub").subscribe();

                producer.send("hello");
                Message<String> msg = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(msg).isNotNull();
                assertThat(msg.getValue()).isEqualTo("hello");
            }

        } finally {
            standalone.close();
        }
        assertThat(tempBaseDir).as("Temp directory was not cleaned up").doesNotExist();
    }
}
