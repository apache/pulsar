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
package org.apache.pulsar.sql.presto.decoder.json;

import com.google.common.collect.ImmutableList;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.*;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.sql.presto.PulsarColumnHandle;
import org.apache.pulsar.sql.presto.PulsarColumnMetadata;
import org.apache.pulsar.sql.presto.PulsarRowDecoderFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * PulsarRowDecoderFactory for {@link org.apache.pulsar.common.schema.SchemaType#JSON}.
 */
public class PulsarJsonRowDecoderFactory implements PulsarRowDecoderFactory {

    private TypeManager typeManager;

    public PulsarJsonRowDecoderFactory(TypeManager typeManager) {
        this.typeManager = typeManager;
    }

    @Override
    public PulsarJsonRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns) {
        return new PulsarJsonRowDecoder((GenericJsonSchema) GenericJsonSchema.of(schemaInfo), columns);
    }

    @Override
    public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        String schemaJson = new String(schemaInfo.getSchema());
        if (StringUtils.isBlank(schemaJson)) {
            throw new PrestoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }
        Schema schema;
        try {
            schema = GenericJsonSchema.of(schemaInfo).getAvroSchema();
        } catch (SchemaParseException ex) {
            throw new PrestoException(NOT_SUPPORTED, "Topic "
                    + topicName.toString() + " does not have a valid schema");
        }

        //TODO : check schema cyclic definitions which may case java.lang.StackOverflowError

        return schema.getFields().stream()
                .map(field ->
                        new PulsarColumnMetadata(field.name(), parseJsonPrestoType(field.name(), field.schema()), field.schema().toString(), null, false, false,
                                handleKeyValueType, new PulsarColumnMetadata.DecoderExtraInfo(field.name(), null, null))

                ).collect(toList());
    }


    private Type parseJsonPrestoType(String fieldname, Schema schema) {
        Schema.Type type = schema.getType();
        LogicalType logicalType  = schema.getLogicalType();
        switch (type) {
            case STRING:
            case ENUM:
                return createUnboundedVarcharType();
            case NULL:
                throw new UnsupportedOperationException(format("field '%s' NULL Type code should not be reached ，please check the schema or report the bug.", fieldname));
            case FIXED:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case INT:
                if (logicalType == LogicalTypes.timeMillis()) {
                    return TIME;
                } else if (logicalType == LogicalTypes.date()) {
                    return DATE;
                }
                return IntegerType.INTEGER;
            case LONG:
                if (logicalType == LogicalTypes.timestampMillis()) {
                    return TimestampType.TIMESTAMP;
                }
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case ARRAY:
                return new ArrayType(parseJsonPrestoType(fieldname, schema.getElementType()));
            case MAP:
                //The key for an avro map must be a string
                TypeSignature valueType = parseJsonPrestoType(fieldname, schema.getValueType()).getTypeSignature();
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.typeParameter(VarcharType.VARCHAR.getTypeSignature()), TypeSignatureParameter.typeParameter(valueType)));
            case RECORD:
                if (schema.getFields().size() > 0) {
                    return RowType.from(schema.getFields().stream()
                            .map(field -> new RowType.Field(Optional.of(field.name()), parseJsonPrestoType(field.name(), field.schema())))
                            .collect(toImmutableList()));
                } else {
                    throw new UnsupportedOperationException(format("field '%s' of record Type has no fields, please check avro schema definition. ", fieldname));
                }
            case UNION:
                for (Schema nestType : schema.getTypes()) {
                    if (nestType.getType() != Schema.Type.NULL) {
                        return parseJsonPrestoType(fieldname, nestType);
                    }
                }
                throw new UnsupportedOperationException(format("field '%s' of UNION type must contains not null type ", fieldname));
            default:
                throw new UnsupportedOperationException(format("Cannot convert from Schema type '%s' (%s) to Presto type", schema.getType(), schema.getFullName()));
        }
    }
}
