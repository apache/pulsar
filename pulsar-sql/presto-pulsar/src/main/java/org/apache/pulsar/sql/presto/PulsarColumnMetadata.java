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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import java.util.Arrays;
import java.util.Objects;

/**
 * Description of the column metadata.
 */
public class PulsarColumnMetadata extends ColumnMetadata {

    private boolean isInternal;
    // need this because presto ColumnMetadata saves name in lowercase
    private String nameWithCase;
    private String[] fieldNames;
    private Integer[] positionIndices;
    private PulsarColumnHandle.HandleKeyValueType handleKeyValueType;
    public final static String KEY_SCHEMA_COLUMN_PREFIX = "__key.";

    public PulsarColumnMetadata(String name, Type type, String comment, String extraInfo,
                                boolean hidden, boolean isInternal,
                                String[] fieldNames, Integer[] positionIndices,
                                PulsarColumnHandle.HandleKeyValueType handleKeyValueType) {
        super(name, type, comment, extraInfo, hidden);
        this.nameWithCase = name;
        this.isInternal = isInternal;
        this.fieldNames = fieldNames;
        this.positionIndices = positionIndices;
        this.handleKeyValueType = handleKeyValueType;
    }

    public String getNameWithCase() {
        return nameWithCase;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public Integer[] getPositionIndices() {
        return positionIndices;
    }

    public PulsarColumnHandle.HandleKeyValueType getHandleKeyValueType() {
        return handleKeyValueType;
    }

    public boolean isKey() {
        return Objects.equals(handleKeyValueType, PulsarColumnHandle.HandleKeyValueType.KEY);
    }

    public boolean isValue() {
        return Objects.equals(handleKeyValueType, PulsarColumnHandle.HandleKeyValueType.VALUE);
    }

    public static String getColumnName(PulsarColumnHandle.HandleKeyValueType handleKeyValueType, String name) {
        if (Objects.equals(PulsarColumnHandle.HandleKeyValueType.KEY, handleKeyValueType)) {
            return KEY_SCHEMA_COLUMN_PREFIX + name;
        }
        return name;
    }

    @Override
    public String toString() {
        return "PulsarColumnMetadata{"
            + "isInternal=" + isInternal
            + ", nameWithCase='" + nameWithCase + '\''
            + ", fieldNames=" + Arrays.toString(fieldNames)
            + ", positionIndices=" + Arrays.toString(positionIndices)
            + ", handleKeyValueType=" + handleKeyValueType
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        PulsarColumnMetadata that = (PulsarColumnMetadata) o;

        if (isInternal != that.isInternal) {
            return false;
        }
        if (nameWithCase != null ? !nameWithCase.equals(that.nameWithCase) : that.nameWithCase != null) {
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
        int result = super.hashCode();
        result = 31 * result + (isInternal ? 1 : 0);
        result = 31 * result + (nameWithCase != null ? nameWithCase.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(fieldNames);
        result = 31 * result + Arrays.hashCode(positionIndices);
        result = 31 * result + (handleKeyValueType != null ? handleKeyValueType.hashCode() : 0);
        return result;
    }
}
