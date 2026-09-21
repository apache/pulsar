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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewMessageMapper;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests of {@link MappedTableViewImpl} with a mocked reader.
 */
public class MappedTableViewImplTest {

    private static final String TOPIC = "persistent://public/default/mapped-table-view";

    private PulsarClientImpl client;
    private Reader<String> reader;
    private TableViewConfigurationData conf;
    /** The last message ids returned by the reader, which drive the initial replay and refreshAsync(). */
    private final AtomicReference<List<TopicMessageId>> lastMessageIds = new AtomicReference<>(List.of());

    @BeforeMethod
    @SuppressWarnings("unchecked")
    public void setup() {
        client = mock(PulsarClientImpl.class);
        ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
        reader = mock(Reader.class);
        when(client.newReader(Schema.STRING)).thenReturn(builder);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
        when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(reader.getLastMessageIdsAsync())
                .thenAnswer(invocation -> CompletableFuture.completedFuture(lastMessageIds.get()));
        lastMessageIds.set(List.of());
        conf = new TableViewConfigurationData();
        conf.setTopicName(TOPIC);
    }

    private static TopicMessageId messageId(long entryId) {
        return new TopicMessageIdImpl(TOPIC, new MessageIdImpl(1, entryId, -1));
    }

    @SuppressWarnings("unchecked")
    private static Message<String> message(String key, String value, TopicMessageId messageId) {
        Message<String> message = mock(Message.class);
        when(message.getTopicName()).thenReturn(TOPIC);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.hasKey()).thenReturn(true);
        when(message.getKey()).thenReturn(key);
        when(message.size()).thenReturn(1);
        when(message.getValue()).thenReturn(value);
        return message;
    }

    /** Maps the payload to upper case and fails for the payload "boom". */
    private static String mapOrFail(Message<String> message) throws IOException {
        if ("boom".equals(message.getValue())) {
            throw new IOException("boom");
        }
        return message.getValue().toUpperCase();
    }

    @Test(timeOut = 10_000)
    public void testMappingErrorSkipsMessageAndNotifiesMapper() throws Exception {
        List<Message<String>> failedMessages = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();
        AtomicBoolean callbackRefreshCompleted = new AtomicBoolean();
        AtomicReference<TableView<String>> tableViewRef = new AtomicReference<>();
        TableViewMessageMapper<String, String> mapper = new TableViewMessageMapper<>() {
            @Override
            public String map(Message<String> message) throws IOException {
                return mapOrFail(message);
            }

            @Override
            public boolean onMappingError(Message<String> message, Throwable error) {
                failedMessages.add(message);
                errors.add(error);
                // A refresh from the callback must observe the skipped message as processed
                callbackRefreshCompleted.set(tableViewRef.get().refreshAsync().isDone());
                return true;
            }
        };
        CompletableFuture<Message<String>> first = new CompletableFuture<>();
        CompletableFuture<Message<String>> second = new CompletableFuture<>();
        CompletableFuture<Message<String>> third = new CompletableFuture<>();
        when(reader.readNextAsync()).thenReturn(first, second, third, new CompletableFuture<>());
        List<String> listened = new ArrayList<>();

        TableView<String> tableView = new MappedTableViewImpl<>(client, Schema.STRING, conf, mapper)
                .start().get(5, TimeUnit.SECONDS);
        tableViewRef.set(tableView);
        tableView.listen((key, value) -> listened.add(key + "=" + value));

        first.complete(message("key", "first", messageId(0)));
        assertThat(tableView.get("key")).isEqualTo("FIRST");

        Message<String> poison = message("key", "boom", messageId(1));
        lastMessageIds.set(List.of(messageId(1)));
        second.complete(poison);
        assertThat(tableView.get("key")).as("the key keeps its previous value").isEqualTo("FIRST");
        assertThat(failedMessages).containsExactly(poison);
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).isInstanceOf(IOException.class).hasMessage("boom");
        assertThat(callbackRefreshCompleted).isTrue();
        assertThat(tableView.refreshAsync()).as("refresh does not wait for the skipped message").isDone();
        assertThat(listened).as("listeners are not notified for the skipped message").containsExactly("key=FIRST");

        third.complete(message("key", "third", messageId(2)));
        assertThat(tableView.get("key")).as("the view keeps applying later messages").isEqualTo("THIRD");
        assertThat(listened).containsExactly("key=FIRST", "key=THIRD");
        tableView.close();
    }

    @Test(timeOut = 10_000)
    public void testMappingErrorWithDefaultCallbackSkipsMessage() throws Exception {
        CompletableFuture<Message<String>> first = new CompletableFuture<>();
        CompletableFuture<Message<String>> second = new CompletableFuture<>();
        when(reader.readNextAsync()).thenReturn(first, second, new CompletableFuture<>());

        TableView<String> tableView = new MappedTableViewImpl<String, String>(client, Schema.STRING, conf,
                MappedTableViewImplTest::mapOrFail).start().get(5, TimeUnit.SECONDS);

        first.complete(message("key", "boom", messageId(0)));
        assertThat(tableView.containsKey("key")).isFalse();

        second.complete(message("key", "second", messageId(1)));
        assertThat(tableView.get("key")).isEqualTo("SECOND");
        tableView.close();
    }

    @Test(timeOut = 10_000)
    public void testMappingErrorCallbackExceptionIsIgnored() throws Exception {
        TableViewMessageMapper<String, String> mapper = new TableViewMessageMapper<>() {
            @Override
            public String map(Message<String> message) throws IOException {
                return mapOrFail(message);
            }

            @Override
            public boolean onMappingError(Message<String> message, Throwable error) {
                throw new IllegalStateException("callback failure");
            }
        };
        CompletableFuture<Message<String>> first = new CompletableFuture<>();
        CompletableFuture<Message<String>> second = new CompletableFuture<>();
        when(reader.readNextAsync()).thenReturn(first, second, new CompletableFuture<>());

        TableView<String> tableView = new MappedTableViewImpl<>(client, Schema.STRING, conf, mapper)
                .start().get(5, TimeUnit.SECONDS);

        first.complete(message("key", "boom", messageId(0)));
        assertThat(tableView.containsKey("key")).isFalse();

        second.complete(message("key", "second", messageId(1)));
        assertThat(tableView.get("key")).isEqualTo("SECOND");
        tableView.close();
    }

    @Test(timeOut = 10_000)
    public void testMappingErrorDuringReplayDoesNotFailStart() throws Exception {
        List<Message<String>> failedMessages = new ArrayList<>();
        TableViewMessageMapper<String, String> mapper = new TableViewMessageMapper<>() {
            @Override
            public String map(Message<String> message) throws IOException {
                return mapOrFail(message);
            }

            @Override
            public boolean onMappingError(Message<String> message, Throwable error) {
                failedMessages.add(message);
                return true;
            }
        };
        Message<String> poison = message("key", "boom", messageId(0));
        lastMessageIds.set(List.of(messageId(0)));
        when(reader.hasMessageAvailableAsync()).thenReturn(CompletableFuture.completedFuture(true));
        when(reader.readNextAsync()).thenReturn(CompletableFuture.completedFuture(poison),
                new CompletableFuture<>());

        TableView<String> tableView = new MappedTableViewImpl<>(client, Schema.STRING, conf, mapper)
                .start().get(5, TimeUnit.SECONDS);

        assertThat(failedMessages).containsExactly(poison);
        assertThat(tableView.isEmpty()).isTrue();
        tableView.close();
    }

    @Test(timeOut = 10_000)
    public void testStartClosesReaderWhenReplayFails() {
        lastMessageIds.set(List.of(messageId(0)));
        when(reader.hasMessageAvailableAsync()).thenReturn(CompletableFuture.completedFuture(true));
        when(reader.readNextAsync())
                .thenReturn(FutureUtil.failedFuture(new PulsarClientException("read failure")));

        CompletableFuture<TableView<String>> start =
                new MappedTableViewImpl<String, String>(client, Schema.STRING, conf, Message::getValue).start();

        assertThatThrownBy(() -> start.get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(PulsarClientException.class).hasMessage("read failure");
        verify(reader).closeAsync();
    }

    @Test(timeOut = 10_000)
    public void testStartFailsWhenHasMessageAvailableFails() {
        lastMessageIds.set(List.of(messageId(0)));
        when(reader.hasMessageAvailableAsync())
                .thenReturn(FutureUtil.failedFuture(new PulsarClientException("check failure")));

        CompletableFuture<TableView<String>> start =
                new MappedTableViewImpl<String, String>(client, Schema.STRING, conf, Message::getValue).start();

        // Without the failure propagation the replay never completes and create() would wait forever
        assertThatThrownBy(() -> start.get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(PulsarClientException.class).hasMessage("check failure");
        verify(reader).closeAsync();
    }

    @Test
    public void testConstructorRejectsCompactionStrategyBeforeCreatingReader() {
        conf.setTopicCompactionStrategyClassName(TableViewBuilderImplTest.NoopStrategy.class.getName());

        assertThatThrownBy(() -> new MappedTableViewImpl<String, String>(client, Schema.STRING, conf,
                Message::getValue))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topicCompactionStrategyClassName");
        verify(client, never()).newReader(any(Schema.class));
    }
}
