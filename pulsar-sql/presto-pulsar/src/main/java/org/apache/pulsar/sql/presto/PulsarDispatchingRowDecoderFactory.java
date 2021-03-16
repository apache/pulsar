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

import static java.lang.String.format;

import com.google.inject.Inject;

import io.airlift.log.Logger;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.TypeManager;

import java.util.List;
import java.util.Set;

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

    private TypeManager typeManager;

    @Inject
    public PulsarDispatchingRowDecoderFactory(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo,
                                             Set<DecoderColumnHandle> columns) {
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.createRowDecoder(topicName, schemaInfo, columns);
    }

    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo,
                                                      PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        PulsarRowDecoderFactory rowDecoderFactory = createDecoderFactory(schemaInfo);
        return rowDecoderFactory.extractColumnMetadata(topicName, schemaInfo, handleKeyValueType);
    }

    private PulsarRowDecoderFactory createDecoderFactory(SchemaInfo schemaInfo) {
        if (SchemaType.AVRO.equals(schemaInfo.getType())) {
            return new PulsarAvroRowDecoderFactory(typeManager);
        } else if (SchemaType.JSON.equals(schemaInfo.getType())) {
            return new PulsarJsonRowDecoderFactory(typeManager);
        }else if (SchemaType.PROTOBUF_NATIVE.equals(schemaInfo.getType())) {
            return new PulsarProtobufNativeRowDecoderFactory(typeManager);
        } else if (schemaInfo.getType().isPrimitive()) {
            return new PulsarPrimitiveRowDecoderFactory();
        } else {
            throw new RuntimeException(format("'%s' is unsupported type '%s'", schemaInfo.getName(),
                    schemaInfo.getType()));
        }
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }

}