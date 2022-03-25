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
package org.apache.pulsar.sql.presto.decoder.protobufnative;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.stream.Collectors.toList;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.TimestampProto;
import io.airlift.log.Logger;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.sql.presto.PulsarColumnHandle;
import org.apache.pulsar.sql.presto.PulsarColumnMetadata;
import org.apache.pulsar.sql.presto.PulsarRowDecoder;
import org.apache.pulsar.sql.presto.PulsarRowDecoderFactory;

/**
 * PulsarRowDecoderFactory for {@link org.apache.pulsar.common.schema.SchemaType#PROTOBUF_NATIVE}.
 */
public class PulsarProtobufNativeRowDecoderFactory implements PulsarRowDecoderFactory {

    private TypeManager typeManager;

    public PulsarProtobufNativeRowDecoderFactory(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    @Override
    public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo,
                                             Set<DecoderColumnHandle> columns) {
        return new PulsarProtobufNativeRowDecoder((GenericProtobufNativeSchema)
                GenericProtobufNativeSchema.of(schemaInfo), columns);
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo,
                                                      PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        List<ColumnMetadata> columnMetadata;
        String schemaJson = new String(schemaInfo.getSchema());
        if (StringUtils.isBlank(schemaJson)) {
            throw new PrestoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }
        Descriptors.Descriptor schema;
        try {
            schema =
                    ((GenericProtobufNativeSchema) GenericProtobufNativeSchema.of(schemaInfo))
                            .getProtobufNativeSchema();
        } catch (Exception ex) {
            log.error(ex);
            throw new PrestoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }

        //Protobuf have not yet supported Cyclic Objects.
        columnMetadata = schema.getFields().stream()
                .map(field ->
                        new PulsarColumnMetadata(PulsarColumnMetadata.getColumnName(handleKeyValueType,
                                field.getName()), parseProtobufPrestoType(field), field.getType().toString(), null,
                                false, false, handleKeyValueType,
                                new PulsarColumnMetadata.DecoderExtraInfo(field.getName(), null, null))

                ).collect(toList());

        return columnMetadata;
    }

    private Type parseProtobufPrestoType(Descriptors.FieldDescriptor field) {
        //parse by proto JavaType
        Descriptors.FieldDescriptor.JavaType type = field.getJavaType();
        Type dataType;
        switch (type) {
            case BOOLEAN:
                dataType = BooleanType.BOOLEAN;
                break;
            case BYTE_STRING:
                dataType = VarbinaryType.VARBINARY;
                break;
            case DOUBLE:
                dataType = DoubleType.DOUBLE;
                break;
            case ENUM:
            case STRING:
                dataType = createUnboundedVarcharType();
                break;
            case FLOAT:
                dataType = RealType.REAL;
                break;
            case INT:
                dataType = IntegerType.INTEGER;
                break;
            case LONG:
                dataType = BigintType.BIGINT;
                break;
            case MESSAGE:
                Descriptors.Descriptor msg = field.getMessageType();
                if (field.isMapField()) {
                    //map
                    TypeSignature keyType =
                            parseProtobufPrestoType(msg.findFieldByName(PulsarProtobufNativeColumnDecoder
                                    .PROTOBUF_MAP_KEY_NAME)).getTypeSignature();
                    TypeSignature valueType =
                            parseProtobufPrestoType(msg.findFieldByName(PulsarProtobufNativeColumnDecoder
                                    .PROTOBUF_MAP_VALUE_NAME)).getTypeSignature();
                    return typeManager.getParameterizedType(StandardTypes.MAP,
                            ImmutableList.of(TypeSignatureParameter.typeParameter(keyType),
                                    TypeSignatureParameter.typeParameter(valueType)));
                } else {
                    if (TimestampProto.getDescriptor().toProto().getName().equals(msg.getFile().toProto().getName())) {
                        //if msg type is protobuf/timestamp
                        dataType = TimestampType.TIMESTAMP;
                    } else {
                        //row
                        dataType = RowType.from(msg.getFields().stream()
                                .map(rowField -> new RowType.Field(Optional.of(rowField.getName()),
                                        parseProtobufPrestoType(rowField)))
                                .collect(toImmutableList()));
                    }
                }
                break;
            default:
                throw new RuntimeException("Unknown type: " + type.toString() + " for FieldDescriptor: "
                        + field.getName());
        }
        //list
        if (field.isRepeated() && !field.isMapField()) {
            dataType = new ArrayType(dataType);
        }

        return dataType;
    }

    private static final Logger log = Logger.get(PulsarProtobufNativeRowDecoderFactory.class);

}
