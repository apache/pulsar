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
package org.apache.pulsar.sql.presto;

import static java.lang.String.format;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TypeManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.sql.presto.decoder.avro.PulsarAvroRowDecoderFactory;
import org.apache.pulsar.sql.presto.decoder.json.PulsarJsonRowDecoderFactory;
import org.apache.pulsar.sql.presto.decoder.primitive.PulsarPrimitiveRowDecoderFactory;
import org.apache.pulsar.sql.presto.decoder.protobufnative.PulsarProtobufNativeRowDecoderFactory;

/**
 * dispatcher RowDecoderFactory for {@link org.apache.pulsar.common.schema.SchemaType}.
 */
public class PulsarDispatchingRowDecoderFactory {

    private static final Logger log = Logger.get(PulsarDispatchingRowDecoderFactory.class);

    private static TypeManager typeManager;

    private static final Map<SchemaType, PulsarRowDecoderFactory> decoderFactories = new HashMap<>();

    private final Map<String, PulsarRowDecoder> decoderCache = new ConcurrentHashMap<>();

    static {
        decoderFactories.put(SchemaType.AVRO, new PulsarAvroRowDecoderFactory(typeManager));
        decoderFactories.put(SchemaType.JSON, new PulsarJsonRowDecoderFactory(typeManager));
        decoderFactories.put(SchemaType.PROTOBUF_NATIVE, new PulsarProtobufNativeRowDecoderFactory(typeManager));
        decoderFactories.put(SchemaType.STRING, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.BOOLEAN, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.INT8, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.INT16, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.INT32, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.INT64, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.FLOAT, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.DOUBLE, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.DATE, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.TIME, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.TIMESTAMP, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.BYTES, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.INSTANT, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.LOCAL_DATE, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.LOCAL_TIME, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.LOCAL_DATE_TIME, new PulsarPrimitiveRowDecoderFactory());
        decoderFactories.put(SchemaType.NONE, new PulsarPrimitiveRowDecoderFactory());
    }

    @Inject
    public PulsarDispatchingRowDecoderFactory(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo,
                                             Set<DecoderColumnHandle> columns) {
        String schemaString = schemaInfo.getSchema().toString();
        PulsarRowDecoder decoder = decoderCache.get(schemaString);
        if (decoder == null) {
            decoder = new PulsarRowDecoder() {
                @Override
                public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf) {
                    return Optional.empty();
                }
            };
            decoderCache.put(schemaString, decoder);
        }
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.createRowDecoder(topicName, schemaInfo, columns);
    }

    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo,
                                                      PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.extractColumnMetadata(topicName, schemaInfo, handleKeyValueType);
    }

    private PulsarRowDecoderFactory createDecoderFactory(SchemaInfo schemaInfo) {
        PulsarRowDecoderFactory decoderFactory = decoderFactories.get(schemaInfo.getType());
        if (decoderFactory == null) {
            throw new RuntimeException(format("'%s' is unsupported type '%s'",
                    schemaInfo.getName(), schemaInfo.getType()));
        }
        return decoderFactory;
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }
}
