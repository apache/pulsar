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
package org.apache.pulsar.io.cassandra.util;

import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.beanutils.converters.NumberConverter;

public abstract class RecordWrapper<T> {

    T recordValue;
    NumberConverter converter = new IntegerConverter();

    public RecordWrapper(T value) {
        this.recordValue = value;
    }

    public abstract Object get(TableMetadataProvider.ColumnId column);

    public abstract boolean containsKey(String name);

    Object getValueAsExpectedType(Object value, TableMetadataProvider.ColumnId column) {
        switch (column.getType().getName()) {
            case FLOAT: return converter.convert(Float.class, value);
            case INT: return converter.convert(Integer.class, value);
            case DOUBLE: return converter.convert(Double.class, value);
            case TEXT: return value.toString();
            default: return value;
        }

    }

}
