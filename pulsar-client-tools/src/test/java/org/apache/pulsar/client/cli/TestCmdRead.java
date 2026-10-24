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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class TestCmdRead {

    @DataProvider(name = "startMessageIds")
    public Object[][] startMessageIds() {
        // The V5-based reader (CheckpointConsumer) only accepts latest / earliest; the
        // <ledgerId>:<entryId> form is no longer supported, so it is not exercised here.
        return new Object[][] {
            { "latest", "latest" },
            { "earliest", "earliest" },
        };
    }

    @Test(dataProvider = "startMessageIds")
    public void testGetWebSocketReadUri(String msgId, String msgIdQueryParam) throws Exception {
        CmdRead cmdRead = new CmdRead();
        cmdRead.updateConfig(null, null, "ws://localhost:8080/");
        Field startMessageIdField = CmdRead.class.getDeclaredField("startMessageId");
        startMessageIdField.setAccessible(true);
        startMessageIdField.set(cmdRead, msgId);

        String topicNameV2 = "persistent://public/default/t2";
        assertEquals(cmdRead.getWebSocketReadUri(topicNameV2),
                "ws://localhost:8080/ws/v2/reader/persistent/public/default/t2?messageId=" + msgIdQueryParam);
    }

    @DataProvider(name = "readCompactedValues")
    public Object[][] readCompactedValues() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "readCompactedValues")
    @SuppressWarnings("unchecked")
    public void testReadCompactedReader(boolean readCompacted) throws Exception {
        ClientBuilder clientBuilder = mock(ClientBuilder.class);
        PulsarClient client = mock(PulsarClient.class);
        ReaderBuilder<ByteBuffer> builder = mock(ReaderBuilder.class, RETURNS_SELF);
        Reader<ByteBuffer> reader = mock(Reader.class);
        Message<ByteBuffer> message = mock(Message.class);
        when(clientBuilder.build()).thenReturn(client);
        when(client.newReader(Schema.BYTEBUFFER)).thenReturn(builder);
        when(builder.create()).thenReturn(reader);
        when(reader.readNext(5, TimeUnit.SECONDS)).thenReturn(message);

        CmdRead command = new CmdRead();
        List<String> args = new ArrayList<>(List.of("--hide-content", "persistent://public/default/t"));
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
        verify(builder).create();
        verify(message).release();
    }

    @DataProvider(name = "unsupportedCompactedReaders")
    public Object[][] unsupportedCompactedReaders() {
        return new Object[][] {
            {"pulsar://localhost:6650", "non-persistent://public/default/t", "persistent topic"},
            {"ws://localhost:8080", "persistent://public/default/t", "binary protocol"},
            {"wss://localhost:8443", "persistent://public/default/t", "binary protocol"},
        };
    }

    @Test(dataProvider = "unsupportedCompactedReaders")
    public void testRejectsUnsupportedCompactedReader(String serviceUrl, String topic, String error) {
        CmdRead command = new CmdRead();
        new CommandLine(command).parseArgs("--read-compacted", topic);
        command.updateConfig(null, null, serviceUrl);

        assertThatThrownBy(command::run)
                .isInstanceOf(CommandLine.ParameterException.class)
                .hasMessageContaining("--read-compacted")
                .hasMessageContaining(error);
    }
}
