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

import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.QUERY_REJECTED;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.util.Objects.requireNonNull;
import static org.apache.pulsar.sql.presto.PulsarConnectorUtils.restoreNamespaceDelimiterIfNeeded;
import static org.apache.pulsar.sql.presto.PulsarConnectorUtils.rewriteNamespaceDelimiterIfNeeded;
import static org.apache.pulsar.sql.presto.PulsarHandleResolver.convertColumnHandle;
import static org.apache.pulsar.sql.presto.PulsarHandleResolver.convertTableHandle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * This connector helps to work with metadata.
 */
public class PulsarMetadata implements ConnectorMetadata {

    private final String connectorId;
    private final PulsarAdmin pulsarAdmin;
    private final PulsarConnectorConfig pulsarConnectorConfig;

    private static final String INFORMATION_SCHEMA = "information_schema";

    private static final Logger log = Logger.get(PulsarMetadata.class);

    @Inject
    public PulsarMetadata(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        List<String> prestoSchemas = new LinkedList<>();
        try {
            List<String> tenants = pulsarAdmin.tenants().getTenants();
            for (String tenant : tenants) {
                prestoSchemas.addAll(pulsarAdmin.namespaces().getNamespaces(tenant).stream().map(namespace ->
                    rewriteNamespaceDelimiterIfNeeded(namespace, pulsarConnectorConfig)).collect(Collectors.toList()));
            }
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 401) {
                throw new PrestoException(QUERY_REJECTED, "Failed to get schemas from pulsar: Unauthorized");
            }
            throw new RuntimeException("Failed to get schemas from pulsar: "
                    + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
        }
        return prestoSchemas;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return new PulsarTableHandle(
                this.connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
                                                            Constraint constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns) {

        PulsarTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(
            new PulsarTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ConnectorTableMetadata connectorTableMetadata;
        SchemaTableName schemaTableName = convertTableHandle(table).toSchemaTableName();
        connectorTableMetadata = getTableMetadata(schemaTableName, true);
        if (connectorTableMetadata == null) {
            ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
            connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, builder.build());
        }
        return connectorTableMetadata;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        if (schemaName.isPresent()) {
            String schemaNameOrNull = schemaName.get();

            if (schemaNameOrNull.equals(INFORMATION_SCHEMA)) {
                // no-op for now but add pulsar connector specific system tables here
            } else {
                List<String> pulsarTopicList = null;
                try {
                    pulsarTopicList = this.pulsarAdmin.topics()
                        .getList(restoreNamespaceDelimiterIfNeeded(schemaNameOrNull, pulsarConnectorConfig));
                } catch (PulsarAdminException e) {
                    if (e.getStatusCode() == 404) {
                        log.warn("Schema " + schemaNameOrNull + " does not exsit");
                        return builder.build();
                    } else if (e.getStatusCode() == 401) {
                        throw new PrestoException(QUERY_REJECTED,
                                String.format("Failed to get tables/topics in %s: Unauthorized", schemaNameOrNull));
                    }
                    throw new RuntimeException("Failed to get tables/topics in " + schemaNameOrNull + ": "
                            + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
                }
                if (pulsarTopicList != null) {
                    pulsarTopicList.stream()
                        .map(topic -> TopicName.get(topic).getPartitionedTopicName())
                        .distinct()
                        .forEach(topic -> builder.add(new SchemaTableName(schemaNameOrNull,
                            TopicName.get(topic).getLocalName())));
                }
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        PulsarTableHandle pulsarTableHandle = convertTableHandle(tableHandle);

        ConnectorTableMetadata tableMetaData = getTableMetadata(pulsarTableHandle.toSchemaTableName(), false);
        if (tableMetaData == null) {
            return new HashMap<>();
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        tableMetaData.getColumns().forEach(columnMetadata -> {

            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) columnMetadata;

            PulsarColumnHandle pulsarColumnHandle = new PulsarColumnHandle(
                    connectorId,
                    pulsarColumnMetadata.getNameWithCase(),
                    pulsarColumnMetadata.getType(),
                    pulsarColumnMetadata.isHidden(),
                    pulsarColumnMetadata.isInternal(),
                    pulsarColumnMetadata.getFieldNames(),
                    pulsarColumnMetadata.getPositionIndices(),
                    pulsarColumnMetadata.getHandleKeyValueType());

            columnHandles.put(
                    columnMetadata.getName(),
                    pulsarColumnHandle);
        });

        PulsarInternalColumn.getInternalFields().forEach(pulsarInternalColumn -> {
            PulsarColumnHandle pulsarColumnHandle = pulsarInternalColumn.getColumnHandle(connectorId, false);
            columnHandles.put(pulsarColumnHandle.getName(), pulsarColumnHandle);
        });

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle
            columnHandle) {

        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix
            prefix) {

        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (!prefix.getTable().isPresent()) {
            tableNames = listTables(session, prefix.getSchema());
        } else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
        }

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata connectorTableMetadata = getTableMetadata(tableName, true);
            if (connectorTableMetadata != null) {
                columns.put(tableName, connectorTableMetadata.getColumns());
            }
        }

        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName, boolean withInternalColumns) {

        if (schemaTableName.getSchemaName().equals(INFORMATION_SCHEMA)) {
            return null;
        }
        String namespace = restoreNamespaceDelimiterIfNeeded(schemaTableName.getSchemaName(), pulsarConnectorConfig);

        TopicName topicName = TopicName.get(
                String.format("%s/%s", namespace, schemaTableName.getTableName()));

        List<String> topics;
        try {
            if (!PulsarConnectorUtils.isPartitionedTopic(topicName, this.pulsarAdmin)) {
                topics = this.pulsarAdmin.topics().getList(namespace);
            } else {
                topics = this.pulsarAdmin.topics().getPartitionedTopicList(namespace);
            }
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                throw new PrestoException(NOT_FOUND, "Schema " + namespace + " does not exist");
            } else if (e.getStatusCode() == 401) {
                throw new PrestoException(QUERY_REJECTED,
                        String.format("Failed to get topics in schema %s: Unauthorized", namespace));
            }
            throw new RuntimeException("Failed to get topics in schema " + namespace
                    + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
        }

        if (!topics.contains(topicName.toString())) {
            log.error("Table %s not found",
                    String.format("%s/%s", namespace,
                            schemaTableName.getTableName()));
            throw new TableNotFoundException(schemaTableName);
        }

        SchemaInfo schemaInfo;
        try {
            schemaInfo = this.pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", namespace, schemaTableName.getTableName()));
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                // use default schema because there is no schema
                schemaInfo = PulsarSchemaHandlers.defaultSchema();
            } else if (e.getStatusCode() == 401) {
                throw new PrestoException(QUERY_REJECTED,
                        String.format("Failed to get pulsar topic schema information for topic %s/%s: Unauthorized",
                                namespace, schemaTableName.getTableName()));
            } else {
                throw new RuntimeException("Failed to get pulsar topic schema information for topic "
                        + String.format("%s/%s", namespace, schemaTableName.getTableName())
                        + ": " + ExceptionUtils.getRootCause(e).getLocalizedMessage(), e);
            }
        }
        List<ColumnMetadata> handles = getPulsarColumns(
                topicName, schemaInfo, withInternalColumns, PulsarColumnHandle.HandleKeyValueType.NONE
        );


        return new ConnectorTableMetadata(schemaTableName, handles);
    }

    /**
     * Convert pulsar schema into presto table metadata.
     */
    static List<ColumnMetadata> getPulsarColumns(TopicName topicName,
                                                 SchemaInfo schemaInfo,
                                                 boolean withInternalColumns,
                                                 PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        SchemaType schemaType = schemaInfo.getType();
        if (schemaType.isStruct()) {
            return getPulsarColumnsFromStructSchema(topicName, schemaInfo, withInternalColumns, handleKeyValueType);
        } else if (schemaType.isPrimitive()) {
            return getPulsarColumnsFromPrimitiveSchema(topicName, schemaInfo, withInternalColumns, handleKeyValueType);
        } else if (schemaType.equals(SchemaType.KEY_VALUE)) {
            return getPulsarColumnsFromKeyValueSchema(topicName, schemaInfo, withInternalColumns);
        } else {
            throw new IllegalArgumentException("Unsupported schema : " + schemaInfo);
        }
    }

    static List<ColumnMetadata> getPulsarColumnsFromPrimitiveSchema(TopicName topicName,
                                                                    SchemaInfo schemaInfo,
                                                                    boolean withInternalColumns,
                                        PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        ColumnMetadata valueColumn = new PulsarColumnMetadata(
                PulsarColumnMetadata.getColumnName(handleKeyValueType, "__value__"),
                convertPulsarType(schemaInfo.getType()),
                "The value of the message with primitive type schema", null, false, false,
                new String[0],
                new Integer[0], handleKeyValueType);

        builder.add(valueColumn);

        if (withInternalColumns) {
            PulsarInternalColumn.getInternalFields()
                    .stream()
                    .forEach(pulsarInternalColumn -> builder.add(pulsarInternalColumn.getColumnMetadata(false)));
        }

        return builder.build();
    }

    static List<ColumnMetadata> getPulsarColumnsFromStructSchema(TopicName topicName,
                                                                 SchemaInfo schemaInfo,
                                                                 boolean withInternalColumns,
                                     PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        String schemaJson = new String(schemaInfo.getSchema());
        if (StringUtils.isBlank(schemaJson)) {
            throw new PrestoException(NOT_SUPPORTED, "Topic " + topicName.toString()
                    + " does not have a valid schema");
        }
        Schema schema;
        try {
            schema = PulsarConnectorUtils.parseSchema(schemaJson);
        } catch (SchemaParseException ex) {
            throw new PrestoException(NOT_SUPPORTED, "Topic " + topicName.toString()
                    + " does not have a valid schema");
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        builder.addAll(getColumns(null, schema, new HashSet<>(), new Stack<>(), new Stack<>(), handleKeyValueType));

        if (withInternalColumns) {
            PulsarInternalColumn.getInternalFields()
                    .stream()
                    .forEach(pulsarInternalColumn -> builder.add(pulsarInternalColumn.getColumnMetadata(false)));
        }
         return builder.build();
    }

    static List<ColumnMetadata> getPulsarColumnsFromKeyValueSchema(TopicName topicName,
                                                                   SchemaInfo schemaInfo,
                                                                   boolean withInternalColumns) {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
        SchemaInfo keySchemaInfo = kvSchemaInfo.getKey();
        List<ColumnMetadata> keyColumnMetadataList = getPulsarColumns(topicName, keySchemaInfo, false,
                PulsarColumnHandle.HandleKeyValueType.KEY);
        builder.addAll(keyColumnMetadataList);

        SchemaInfo valueSchemaInfo = kvSchemaInfo.getValue();
        List<ColumnMetadata> valueColumnMetadataList = getPulsarColumns(topicName, valueSchemaInfo, false,
                PulsarColumnHandle.HandleKeyValueType.VALUE);
        builder.addAll(valueColumnMetadataList);

        if (withInternalColumns) {
            PulsarInternalColumn.getInternalFields()
                    .forEach(pulsarInternalColumn -> builder.add(pulsarInternalColumn.getColumnMetadata(false)));
        }
        return builder.build();
    }

    @VisibleForTesting
    static Type convertPulsarType(SchemaType pulsarType) {
        switch (pulsarType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT8:
                return TinyintType.TINYINT;
            case INT16:
                return SmallintType.SMALLINT;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case NONE:
            case BYTES:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            case DATE:
                return DateType.DATE;
            case TIME:
                return TimeType.TIME;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            default:
                log.error("Cannot convert type: %s", pulsarType);
                return null;
        }
    }


    @VisibleForTesting
    static List<PulsarColumnMetadata> getColumns(String fieldName, Schema fieldSchema,
                                                 Set<String> fieldTypes,
                                                 Stack<String> fieldNames,
                                                 Stack<Integer> positionIndices,
                                                 PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {

        List<PulsarColumnMetadata> columnMetadataList = new LinkedList<>();

        if (isPrimitiveType(fieldSchema.getType())) {
            columnMetadataList.add(new PulsarColumnMetadata(
                    PulsarColumnMetadata.getColumnName(handleKeyValueType, fieldName),
                    convertType(fieldSchema.getType(), fieldSchema.getLogicalType()),
                    null, null, false, false,
                    fieldNames.toArray(new String[fieldNames.size()]),
                    positionIndices.toArray(new Integer[positionIndices.size()]), handleKeyValueType));
        } else if (fieldSchema.getType() == Schema.Type.UNION) {
            boolean canBeNull = false;
            for (Schema type : fieldSchema.getTypes()) {
                if (isPrimitiveType(type.getType())) {
                    PulsarColumnMetadata columnMetadata;
                    if (type.getType() != Schema.Type.NULL) {
                        if (!canBeNull) {
                            columnMetadata = new PulsarColumnMetadata(
                                    PulsarColumnMetadata.getColumnName(handleKeyValueType, fieldName),
                                    convertType(type.getType(), type.getLogicalType()),
                                    null, null, false, false,
                                    fieldNames.toArray(new String[fieldNames.size()]),
                                    positionIndices.toArray(new Integer[positionIndices.size()]), handleKeyValueType);
                        } else {
                            columnMetadata = new PulsarColumnMetadata(
                                    PulsarColumnMetadata.getColumnName(handleKeyValueType, fieldName),
                                    convertType(type.getType(), type.getLogicalType()),
                                    "field can be null", null, false, false,
                                    fieldNames.toArray(new String[fieldNames.size()]),
                                    positionIndices.toArray(new Integer[positionIndices.size()]), handleKeyValueType);
                        }
                        columnMetadataList.add(columnMetadata);
                    } else {
                        canBeNull = true;
                    }
                } else {
                    List<PulsarColumnMetadata> columns = getColumns(fieldName, type, fieldTypes, fieldNames,
                        positionIndices, handleKeyValueType);
                    columnMetadataList.addAll(columns);
                }
            }
        } else if (fieldSchema.getType() == Schema.Type.RECORD) {
            // check if we have seen this type before to prevent cyclic class definitions.
            if (!fieldTypes.contains(fieldSchema.getFullName())) {
                // add to types seen so far in traversal
                fieldTypes.add(fieldSchema.getFullName());
                List<Schema.Field> fields = fieldSchema.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    Schema.Field field = fields.get(i);
                    fieldNames.push(field.name());
                    positionIndices.push(i);
                    List<PulsarColumnMetadata> columns;
                    if (fieldName == null) {
                        columns = getColumns(field.name(), field.schema(), fieldTypes, fieldNames, positionIndices,
                                handleKeyValueType);
                    } else {
                        columns = getColumns(String.format("%s.%s", fieldName, field.name()), field.schema(),
                            fieldTypes, fieldNames, positionIndices, handleKeyValueType);

                    }
                    positionIndices.pop();
                    fieldNames.pop();
                    columnMetadataList.addAll(columns);
                }
                fieldTypes.remove(fieldSchema.getFullName());
            } else {
                log.debug("Already seen type: %s", fieldSchema.getFullName());
            }
        } else if (fieldSchema.getType() == Schema.Type.ARRAY) {

        } else if (fieldSchema.getType() == Schema.Type.MAP) {

        } else if (fieldSchema.getType() == Schema.Type.ENUM) {
            PulsarColumnMetadata columnMetadata = new PulsarColumnMetadata(
                    PulsarColumnMetadata.getColumnName(handleKeyValueType, fieldName),
                    convertType(fieldSchema.getType(), fieldSchema.getLogicalType()),
                    null, null, false, false,
                    fieldNames.toArray(new String[fieldNames.size()]),
                    positionIndices.toArray(new Integer[positionIndices.size()]), handleKeyValueType);
            columnMetadataList.add(columnMetadata);

        } else if (fieldSchema.getType() == Schema.Type.FIXED) {

        } else {
            log.error("Unknown column type: {}", fieldSchema);
        }
        return columnMetadataList;
    }

    @VisibleForTesting
    static Type convertType(Schema.Type avroType, LogicalType logicalType) {
        switch (avroType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT:
                if (logicalType == LogicalTypes.timeMillis()) {
                    return TIME;
                } else if (logicalType == LogicalTypes.date()) {
                    return DATE;
                }
                return IntegerType.INTEGER;
            case LONG:
                if (logicalType == LogicalTypes.timestampMillis()) {
                    return TIMESTAMP;
                }
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BYTES:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            case ENUM:
                return VarcharType.VARCHAR;
            default:
                log.error("Cannot convert type: %s", avroType);
                return null;
        }
    }

    @VisibleForTesting
    static boolean isPrimitiveType(Schema.Type type) {
        return Schema.Type.NULL == type
                || Schema.Type.BOOLEAN == type
                || Schema.Type.INT == type
                || Schema.Type.LONG == type
                || Schema.Type.FLOAT == type
                || Schema.Type.DOUBLE == type
                || Schema.Type.BYTES == type
                || Schema.Type.STRING == type;

    }
}
