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

import static java.util.Objects.requireNonNull;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.predicate.TupleDomain;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * This class represents information for a split.
 */
public class PulsarSplit implements ConnectorSplit {

    private static final Logger log = Logger.get(PulsarSplit.class);

    private final long splitId;
    private final String connectorId;
    private final String schemaName;
    private final String originSchemaName;
    private final String tableName;
    private final long splitSize;
    private final String schema;
    private final SchemaType schemaType;
    private final long startPositionEntryId;
    private final long endPositionEntryId;
    private final long startPositionLedgerId;
    private final long endPositionLedgerId;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final SchemaInfo schemaInfo;

    private final PositionImpl startPosition;
    private final PositionImpl endPosition;
    private final String schemaInfoProperties;

    private final OffloadPoliciesImpl offloadPolicies;

    @JsonCreator
    public PulsarSplit(
            @JsonProperty("splitId") long splitId,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("originSchemaName") String originSchemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("splitSize") long splitSize,
            @JsonProperty("schema") String schema,
            @JsonProperty("schemaType") SchemaType schemaType,
            @JsonProperty("startPositionEntryId") long startPositionEntryId,
            @JsonProperty("endPositionEntryId") long endPositionEntryId,
            @JsonProperty("startPositionLedgerId") long startPositionLedgerId,
            @JsonProperty("endPositionLedgerId") long endPositionLedgerId,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("schemaInfoProperties") String schemaInfoProperties,
            @JsonProperty("offloadPolicies") OffloadPoliciesImpl offloadPolicies) throws IOException {
        this.splitId = splitId;
        requireNonNull(schemaName, "schema name is null");
        this.originSchemaName = originSchemaName;
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.splitSize = splitSize;
        this.schema = schema;
        this.schemaType = schemaType;
        this.startPositionEntryId = startPositionEntryId;
        this.endPositionEntryId = endPositionEntryId;
        this.startPositionLedgerId = startPositionLedgerId;
        this.endPositionLedgerId = endPositionLedgerId;
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.startPosition = PositionImpl.get(startPositionLedgerId, startPositionEntryId);
        this.endPosition = PositionImpl.get(endPositionLedgerId, endPositionEntryId);
        this.schemaInfoProperties = schemaInfoProperties;
        this.offloadPolicies = offloadPolicies;

        ObjectMapper objectMapper = new ObjectMapper();
        this.schemaInfo = SchemaInfo.builder()
                .name(originSchemaName)
                .type(schemaType)
                .schema(schema.getBytes("ISO8859-1"))
                .properties(objectMapper.readValue(schemaInfoProperties, Map.class))
                .build();
    }

    @JsonProperty
    public long getSplitId() {
        return splitId;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public SchemaType getSchemaType() {
        return schemaType;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public long getSplitSize() {
        return splitSize;
    }

    @JsonProperty
    public String getOriginSchemaName() {
        return originSchemaName;
    }

    @JsonProperty
    public String getSchema() {
        return schema;
    }

    @JsonProperty
    public long getStartPositionEntryId() {
        return startPositionEntryId;
    }

    @JsonProperty
    public long getEndPositionEntryId() {
        return endPositionEntryId;
    }

    @JsonProperty
    public long getStartPositionLedgerId() {
        return startPositionLedgerId;
    }

    @JsonProperty
    public long getEndPositionLedgerId() {
        return endPositionLedgerId;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    public PositionImpl getStartPosition() {
        return startPosition;
    }

    public PositionImpl getEndPosition() {
        return endPosition;
    }

    @JsonProperty
    public String getSchemaInfoProperties() {
        return schemaInfoProperties;
    }

    @JsonProperty
    public OffloadPoliciesImpl getOffloadPolicies() {
        return offloadPolicies;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of(HostAddress.fromParts("localhost", 12345));
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public String toString() {
        return "PulsarSplit{"
            + "splitId=" + splitId
            + ", connectorId='" + connectorId + '\''
            + ", originSchemaName='" + originSchemaName + '\''
            + ", schemaName='" + schemaName + '\''
            + ", tableName='" + tableName + '\''
            + ", splitSize=" + splitSize
            + ", schema='" + schema + '\''
            + ", schemaType=" + schemaType
            + ", startPositionEntryId=" + startPositionEntryId
            + ", endPositionEntryId=" + endPositionEntryId
            + ", startPositionLedgerId=" + startPositionLedgerId
            + ", endPositionLedgerId=" + endPositionLedgerId
            + ", schemaInfoProperties=" + schemaInfoProperties
            + (offloadPolicies == null ? "" : offloadPolicies.toString())
            + '}';
    }

    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
