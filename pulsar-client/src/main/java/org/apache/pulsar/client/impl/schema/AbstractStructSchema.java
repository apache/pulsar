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
package org.apache.pulsar.client.impl.schema;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.schema.reader.AbstractMultiVersionReader;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * minimal abstract StructSchema
 */
public abstract class AbstractStructSchema<T> extends AbstractSchema<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractStructSchema.class);

    protected final SchemaInfo schemaInfo;
    protected SchemaReader<T> reader;
    protected SchemaWriter<T> writer;
    protected SchemaInfoProvider schemaInfoProvider;

    public AbstractStructSchema(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
    }


    @Override
    public byte[] encode(T message) {
        return writer.write(message);
    }

    @Override
    public T decode(byte[] bytes) {
        return reader.read(bytes);
    }

    @Override
    public T decode(byte[] bytes, byte[] schemaVersion) {
        return reader.read(bytes, schemaVersion);
    }

    @Override
    public T decode(ByteBuf byteBuf) {
        return reader.read(new ByteBufInputStream(byteBuf));
    }

    @Override
    public T decode(ByteBuf byteBuf, byte[] schemaVersion) {
        return reader.read(new ByteBufInputStream(byteBuf), schemaVersion);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        if (reader != null) {
            this.reader.setSchemaInfoProvider(schemaInfoProvider);
        }
        this.schemaInfoProvider = schemaInfoProvider;
    }

    @Override
    public Schema<T> atSchemaVersion(byte[] schemaVersion) throws SchemaSerializationException {
        Objects.requireNonNull(schemaVersion);
        if (schemaInfoProvider == null) {
            // this schema is not downloaded from the registry
            return this;
        }
        try {
            SchemaInfo schemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion).get();
            if (schemaInfo == null) {
                throw new SchemaSerializationException("Unknown version "+ BytesSchemaVersion.of(schemaVersion));
            }
            return getAbstractStructSchemaAtVersion(schemaVersion, schemaInfo);
        } catch (ExecutionException err) {
            throw new SchemaSerializationException(err.getCause());
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new SchemaSerializationException(err);
        }
    }

    private static class WrappedVersionedSchema<T> extends AbstractStructSchema<T> {
        private final byte[] schemaVersion;
        private final AbstractStructSchema<T> parent;
        public WrappedVersionedSchema(SchemaInfo schemaInfo, final byte[] schemaVersion,
                                      AbstractStructSchema<T> parent) {
            super(schemaInfo);
            this.schemaVersion = schemaVersion;
            this.writer = null;
            this.reader = parent.reader;
            this.schemaInfoProvider = parent.schemaInfoProvider;
            this.parent = parent;
        }

        @Override
        public boolean requireFetchingSchemaInfo() {
            return true;
        }

        @Override
        public T decode(byte[] bytes) {
            return decode(bytes, schemaVersion);
        }

        @Override
        public T decode(ByteBuf byteBuf) {
            return decode(byteBuf, schemaVersion);
        }

        @Override
        public byte[] encode(T message) {
            throw new UnsupportedOperationException("This schema is not meant to be used for encoding");
        }

        @Override
        public Optional<Object> getNativeSchema() {
            if (reader instanceof AbstractMultiVersionReader) {
                AbstractMultiVersionReader abstractMultiVersionReader = (AbstractMultiVersionReader) reader;
                try {
                    SchemaReader schemaReader = abstractMultiVersionReader.getSchemaReader(schemaVersion);
                    return schemaReader.getNativeSchema();
                } catch (ExecutionException err) {
                    throw new RuntimeException(err.getCause());
                }
            } else {
                return Optional.empty();
            }
        }

        @Override
        public String toString() {
            return "VersionedSchema(type=" + schemaInfo.getType() +
                    ",schemaVersion="+BytesSchemaVersion.of(schemaVersion) +
                    ",name="+schemaInfo.getName()
                    + ")";
        }
    }

    private AbstractStructSchema<T> getAbstractStructSchemaAtVersion(byte[] schemaVersion, SchemaInfo schemaInfo) {
        return new WrappedVersionedSchema<>(schemaInfo, schemaVersion, this);
    }

    protected void setWriter(SchemaWriter<T> writer) {
        this.writer = writer;
    }

    protected void setReader(SchemaReader<T> reader) {
        this.reader = reader;
    }

    protected SchemaReader<T> getReader() {
        return  reader;
    }

}
