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

import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * minimal abstract StructSchema
 */
public abstract class AbstractStructSchema<T> extends AbstractSchema<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractStructSchema.class);

    protected final SchemaInfo schemaInfo;
    protected SchemaReader<T> reader;
    protected SchemaWriter<T> writer;
    protected SchemaInfoProvider schemaInfoProvider;

    public AbstractStructSchema(SchemaInfo schemaInfo){
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
