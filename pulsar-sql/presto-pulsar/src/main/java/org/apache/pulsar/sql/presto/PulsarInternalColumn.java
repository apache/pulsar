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
package org.apache.pulsar.sql.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.pulsar.common.api.raw.RawMessage;

/**
 * This abstract class represents internal columns.
 */
public abstract class PulsarInternalColumn {

    /**
     * Internal column representing the partition.
     */
    public static class PartitionColumn extends PulsarInternalColumn {

        PartitionColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            return null;
        }
    }

    /**
     * Internal column representing the event time.
     */
    public static class EventTimeColumn extends PulsarInternalColumn {

        EventTimeColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            return message.getEventTime() == 0 ? null : message.getEventTime();
        }
    }

    /**
     * Internal column representing the publish time.
     */
    public static class PublishTimeColumn extends PulsarInternalColumn {

        PublishTimeColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            return message.getPublishTime();
        }
    }

    /**
     * Internal column representing the message id.
     */
    public static class MessageIdColumn extends PulsarInternalColumn {

        MessageIdColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            return message.getMessageId().toString();
        }
    }

    /**
     * Internal column representing the sequence id.
     */
    public static class SequenceIdColumn extends PulsarInternalColumn {

        SequenceIdColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            return message.getSequenceId();
        }
    }

    /**
     * Internal column representing the producer name.
     */
    public static class ProducerNameColumn extends PulsarInternalColumn {

        ProducerNameColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            return message.getProducerName();
        }
    }

    /**
     * Internal column representing the key.
     */
    public static class KeyColumn extends PulsarInternalColumn {

        KeyColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            return message.getKey().orElse(null);
        }
    }

    /**
     * Internal column representing the message properties.
     */
    public static class PropertiesColumn extends PulsarInternalColumn {

        private static final ObjectMapper mapper = new ObjectMapper();

        PropertiesColumn(String name, Type type, String comment) {
            super(name, type, comment);
        }

        @Override
        public Object getData(RawMessage message) {
            try {
                return mapper.writeValueAsString(message.getProperties());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static final PartitionColumn PARTITION = new PartitionColumn("__partition__", IntegerType.INTEGER,
        "The partition number which the message belongs to");

    public static final PulsarInternalColumn EVENT_TIME = new EventTimeColumn("__event_time__", TimestampType
            .TIMESTAMP, "Application defined timestamp in milliseconds of when the event occurred");

    public static final PulsarInternalColumn PUBLISH_TIME = new PublishTimeColumn("__publish_time__",
            TimestampType.TIMESTAMP, "The timestamp in milliseconds of when event as published");

    public static final PulsarInternalColumn MESSAGE_ID = new MessageIdColumn("__message_id__", VarcharType.VARCHAR,
            "The message ID of the message used to generate this row");

    public static final PulsarInternalColumn SEQUENCE_ID = new SequenceIdColumn("__sequence_id__", BigintType.BIGINT,
            "The sequence ID of the message used to generate this row");

    public static final PulsarInternalColumn PRODUCER_NAME = new ProducerNameColumn("__producer_name__", VarcharType
            .VARCHAR, "The name of the producer that publish the message used to generate this row");

    public static final PulsarInternalColumn KEY = new KeyColumn("__key__", VarcharType.VARCHAR, "The partition key "
        + "for the topic");

    public static final PulsarInternalColumn PROPERTIES = new PropertiesColumn("__properties__", VarcharType.VARCHAR,
            "User defined properties");

    private final String name;
    private final Type type;
    private final String comment;

    PulsarInternalColumn(
            String name,
            Type type,
            String comment) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    PulsarColumnHandle getColumnHandle(String connectorId, boolean hidden) {
        return new PulsarColumnHandle(connectorId,
                getName(),
                getType(),
                hidden,
                true, null, null, PulsarColumnHandle.HandleKeyValueType.NONE);
    }

    PulsarColumnMetadata getColumnMetadata(boolean hidden) {
        return new PulsarColumnMetadata(name, type, comment, null, hidden, true, null, null,
                PulsarColumnHandle.HandleKeyValueType.NONE);
    }

    public static Set<PulsarInternalColumn> getInternalFields() {
        return ImmutableSet.of(PARTITION, EVENT_TIME, PUBLISH_TIME, MESSAGE_ID, SEQUENCE_ID, PRODUCER_NAME, KEY,
            PROPERTIES);
    }

    public static Map<String, PulsarInternalColumn> getInternalFieldsMap() {
        ImmutableMap.Builder<String, PulsarInternalColumn> builder = ImmutableMap.builder();
        getInternalFields().forEach(new Consumer<PulsarInternalColumn>() {
            @Override
            public void accept(PulsarInternalColumn pulsarInternalColumn) {
                builder.put(pulsarInternalColumn.getName(), pulsarInternalColumn);
            }
        });
        return builder.build();
    }

    public abstract Object getData(RawMessage message);
}
