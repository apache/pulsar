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
package org.apache.pulsar.client.api.schema;

/**
 * A field in a record, consisting of a field name, index, and
 * {@link org.apache.pulsar.client.api.Schema} for the field value.
 */
public class Field {

    /**
     * The field name.
     */
    private final String name;
    /**
     * The index of the field within the record.
     */
    private final int index;

    public Field(String name, int index) {
        this.name = name;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field field = (Field) o;

        if (index != field.index) return false;
        return name.equals(field.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + index;
        return result;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", index=" + index +
                '}';
    }
}
