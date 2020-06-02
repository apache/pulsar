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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Objects;

/**
 * This class represents the basic information about a presto column.
 */
public class PulsarColumnHandle implements ColumnHandle {

    private final String connectorId;

    /**
     * Column Name.
     */
    private final String name;

    /**
     * Column type.
     */
    private final Type type;

    /**
     * True if the column should be hidden.
     */
    private final boolean hidden;

    /**
     * True if the column is internal to the connector and not defined by a topic definition.
     */
    private final boolean internal;

    private final String[] fieldNames;

    private final Integer[] positionIndices;

    private HandleKeyValueType handleKeyValueType;

    /**
     * Column Handle keyValue type, used for keyValue schema.
     */
    public enum HandleKeyValueType {
        /**
         * The handle not for keyValue schema.
         */
        NONE,
        /**
         * The key schema handle for keyValue schema.
         */
        KEY,
        /**
         * The value schema handle for keyValue schema.
         */
        VALUE
    }

    @JsonCreator
    public PulsarColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("internal") boolean internal,
            @JsonProperty("fieldNames") String[] fieldNames,
            @JsonProperty("positionIndices") Integer[] positionIndices,
            @JsonProperty("handleKeyValueType") HandleKeyValueType handleKeyValueType) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.hidden = hidden;
        this.internal = internal;
        this.fieldNames = fieldNames;
        this.positionIndices = positionIndices;
        if (handleKeyValueType == null) {
            this.handleKeyValueType = HandleKeyValueType.NONE;
        } else {
            this.handleKeyValueType = handleKeyValueType;
        }
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public boolean isHidden() {
        return hidden;
    }

    @JsonProperty
    public boolean isInternal() {
        return internal;
    }

    @JsonProperty
    public String[] getFieldNames() {
        return fieldNames;
    }

    @JsonProperty
    public Integer[] getPositionIndices() {
        return positionIndices;
    }

    @JsonProperty
    public HandleKeyValueType getHandleKeyValueType() {
        return handleKeyValueType;
    }

    @JsonIgnore
    public boolean isKey() {
        return Objects.equals(handleKeyValueType, HandleKeyValueType.KEY);
    }

    @JsonIgnore
    public boolean isValue() {
        return Objects.equals(handleKeyValueType, HandleKeyValueType.VALUE);
    }

    ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(name, type, null, hidden);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PulsarColumnHandle that = (PulsarColumnHandle) o;

        if (hidden != that.hidden) {
            return false;
        }
        if (internal != that.internal) {
            return false;
        }
        if (connectorId != null ? !connectorId.equals(that.connectorId) : that.connectorId != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }
        if (!Arrays.deepEquals(fieldNames, that.fieldNames)) {
            return false;
        }
        if (!Arrays.deepEquals(positionIndices, that.positionIndices)) {
            return false;
        }
        return Objects.equals(handleKeyValueType, that.handleKeyValueType);
    }

    @Override
    public int hashCode() {
        int result = connectorId != null ? connectorId.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (hidden ? 1 : 0);
        result = 31 * result + (internal ? 1 : 0);
        result = 31 * result + Arrays.hashCode(fieldNames);
        result = 31 * result + Arrays.hashCode(positionIndices);
        result = 31 * result + (handleKeyValueType != null ? handleKeyValueType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PulsarColumnHandle{"
            + "connectorId='" + connectorId + '\''
            + ", name='" + name + '\''
            + ", type=" + type
            + ", hidden=" + hidden
            + ", internal=" + internal
            + ", fieldNames=" + Arrays.toString(fieldNames)
            + ", positionIndices=" + Arrays.toString(positionIndices)
            + ", handleKeyValueType=" + handleKeyValueType
            + '}';
    }
}
