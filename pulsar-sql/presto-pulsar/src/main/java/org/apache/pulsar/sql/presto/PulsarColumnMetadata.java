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

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import java.util.Objects;

/**
 * Description of the column metadata.
 */
public class PulsarColumnMetadata extends ColumnMetadata {

    private boolean isInternal;
    // need this because presto ColumnMetadata saves name in lowercase
    private String nameWithCase;
    private PulsarColumnHandle.HandleKeyValueType handleKeyValueType;
    public static final String KEY_SCHEMA_COLUMN_PREFIX = "__key.";

    private DecoderExtraInfo decoderExtraInfo;

    public PulsarColumnMetadata(String name, Type type, String comment, String extraInfo,
                                boolean hidden, boolean isInternal,
                                PulsarColumnHandle.HandleKeyValueType handleKeyValueType,
                                DecoderExtraInfo decoderExtraInfo) {
        super(name, type, comment, extraInfo, hidden);
        this.nameWithCase = name;
        this.isInternal = isInternal;
        this.handleKeyValueType = handleKeyValueType;
        this.decoderExtraInfo = decoderExtraInfo;
    }

    public DecoderExtraInfo getDecoderExtraInfo() {
        return decoderExtraInfo;
    }


    public String getNameWithCase() {
        return nameWithCase;
    }

    public boolean isInternal() {
        return isInternal;
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
            + ", handleKeyValueType=" + handleKeyValueType
            + ", decoderExtraInfo=" + decoderExtraInfo.toString()
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
        if (!Objects.equals(nameWithCase, that.nameWithCase)) {
            return false;
        }
        if (!Objects.equals(decoderExtraInfo, that.decoderExtraInfo)) {
            return false;
        }
        return Objects.equals(handleKeyValueType, that.handleKeyValueType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (isInternal ? 1 : 0);
        result = 31 * result + (nameWithCase != null ? nameWithCase.hashCode() : 0);
        result = 31 * result + (decoderExtraInfo != null ? decoderExtraInfo.hashCode() : 0);
        result = 31 * result + (handleKeyValueType != null ? handleKeyValueType.hashCode() : 0);
        return result;
    }


    /**
     * Decoder extra info for {@link org.apache.pulsar.sql.presto.PulsarColumnHandle}
     * used by {@link io.prestosql.decoder.RowDecoder}.
     */
    public static class DecoderExtraInfo {

        public DecoderExtraInfo(String mapping, String dataFormat, String formatHint) {
            this.mapping = mapping;
            this.dataFormat = dataFormat;
            this.formatHint = formatHint;
        }

        public DecoderExtraInfo() {}

        //equals ColumnName in general, may used as alias or embedded field in future.
        private String mapping;
        //reserved dataFormat used by RowDecoder.
        private String dataFormat;
        //reserved formatHint used by RowDecoder.
        private String formatHint;

        public String getMapping() {
            return mapping;
        }

        public void setMapping(String mapping) {
            this.mapping = mapping;
        }

        public String getDataFormat() {
            return dataFormat;
        }

        public void setDataFormat(String dataFormat) {
            this.dataFormat = dataFormat;
        }

        public String getFormatHint() {
            return formatHint;
        }

        public void setFormatHint(String formatHint) {
            this.formatHint = formatHint;
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

            DecoderExtraInfo that = (DecoderExtraInfo) o;

            if (!Objects.equals(mapping, that.mapping)) {
                return false;
            }
            if (!Objects.equals(dataFormat, that.dataFormat)) {
                return false;
            }
            return Objects.equals(formatHint, that.formatHint);
        }

        @Override
        public String  toString() {
            return "DecoderExtraInfo{"
                    + "mapping=" + mapping
                    + ", dataFormat=" + dataFormat
                    + ", formatHint=" + formatHint
                    + '}';
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (mapping != null ? mapping.hashCode() : 0);
            result = 31 * result + (dataFormat != null ? dataFormat.hashCode() : 0);
            result = 31 * result + (formatHint != null ? formatHint.hashCode() : 0);
            return result;
        }

    }


}
