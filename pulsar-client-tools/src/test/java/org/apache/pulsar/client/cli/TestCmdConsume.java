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
package org.apache.pulsar.client.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class TestCmdConsume {

    CmdConsume cmdConsume;

    @BeforeMethod
    public void setUp() {
        cmdConsume = new CmdConsume();
        cmdConsume.updateConfig(null, null, "ws://localhost:8080/");
        cmdConsume.subscriptionName = "my-sub";
    }

    @Test
    public void testGetWebSocketConsumeUri() {
        String topicNameV2 = "persistent://public/default/issue-11067";
        assertEquals(cmdConsume.getWebSocketConsumeUri(topicNameV2),
                "ws://localhost:8080/ws/v2/consumer/persistent/public/default/issue-11067/my-sub"
                        + "?subscriptionType=Exclusive&subscriptionMode=Durable");
    }

    @DataProvider(name = "mixedCaseEnumArgs")
    public Object[][] mixedCaseEnumArgs() {
        // The V5 client enums are uppercase; the CLI must still accept the mixed-case v4 spellings
        // on the (sub)commands. picocli does not propagate case-insensitive parsing to subcommands,
        // so this guards the explicit per-command wiring in PulsarClientTool.
        return new Object[][] {
            {"-p", "Earliest"}, {"-p", "earliest"}, {"-p", "EARLIEST"}, {"-p", "Latest"},
            {"-t", "Exclusive"}, {"-t", "Shared"}, {"-t", "Failover"},
            {"-m", "NonDurable"}, {"-ca", "DISCARD"},
        };
    }

    @Test(dataProvider = "mixedCaseEnumArgs")
    public void testCaseInsensitiveEnumFlags(String flag, String value) {
        Properties properties = new Properties();
        properties.setProperty("serviceUrl", "pulsar://localhost:6650");
        PulsarClientTool tool = new PulsarClientTool(properties);
        // Must not throw a picocli ParameterException for the mixed-case enum value.
        tool.getCommander().parseArgs("consume", "-s", "sub", flag, value,
                "persistent://public/default/t");
    }

    @DataProvider(name = "readCompactedSubscriptions")
    public Object[][] readCompactedSubscriptions() {
        return new Object[][] {
            {true, "Exclusive"}, {true, "Failover"},
            {false, "Exclusive"}, {false, "Failover"},
        };
    }

    @Test(dataProvider = "readCompactedSubscriptions")
    @SuppressWarnings("unchecked")
    public void testReadCompactedConsumer(boolean readCompacted, String subscriptionType) throws Exception {
        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        PulsarClient client = mock(PulsarClient.class);
        ConsumerBuilder<ByteBuffer> builder = mock(ConsumerBuilder.class, RETURNS_SELF);
        Consumer<ByteBuffer> consumer = mock(Consumer.class);
        Message<ByteBuffer> message = mock(Message.class);
        when(clientBuilder.build()).thenReturn(client);
        when(client.newConsumer(Schema.BYTEBUFFER)).thenReturn(builder);
        when(builder.subscribe()).thenReturn(consumer);
        when(consumer.receive(5, TimeUnit.SECONDS)).thenReturn(message);

        CmdConsume command = new CmdConsume();
        List<String> args = new ArrayList<>(List.of("-s", "sub", "-t", subscriptionType,
                "--hide-content", "persistent://public/default/t"));
        if (readCompacted) {
            args.add("--read-compacted");
        }
        new CommandLine(command).parseArgs(args.toArray(String[]::new));
        command.updateConfig(null, null, "pulsar://localhost:6650");
        command.updateV4Config(() -> clientBuilder);

        assertThat(command.run()).isZero();
        if (readCompacted) {
            verify(builder).readCompacted(true);
        } else {
            verify(builder, never()).readCompacted(true);
        }
        verify(builder).subscribe();
        verify(consumer).acknowledge(message);
        verify(message).release();
    }

    @DataProvider(name = "unsupportedCompactedConsumers")
    public Object[][] unsupportedCompactedConsumers() {
        return new Object[][] {
            {"pulsar://localhost:6650", "persistent://public/default/t", "Shared", "Exclusive or Failover"},
            {"pulsar://localhost:6650", "persistent://public/default/t", "Key_Shared", "Exclusive or Failover"},
            {"pulsar://localhost:6650", "non-persistent://public/default/t", "Exclusive", "persistent topic"},
            {"ws://localhost:8080", "persistent://public/default/t", "Exclusive", "binary protocol"},
            {"wss://localhost:8443", "persistent://public/default/t", "Exclusive", "binary protocol"},
        };
    }

    @Test(dataProvider = "unsupportedCompactedConsumers")
    public void testRejectsUnsupportedCompactedConsumer(String serviceUrl, String topic,
                                                       String subscriptionType, String error) {
        CmdConsume command = new CmdConsume();
        new CommandLine(command).parseArgs("-s", "sub", "-t", subscriptionType, "--read-compacted", topic);
        command.updateConfig(null, null, serviceUrl);

        assertThatThrownBy(command::run)
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--read-compacted")
                .hasMessageContaining(error);
    }
}
