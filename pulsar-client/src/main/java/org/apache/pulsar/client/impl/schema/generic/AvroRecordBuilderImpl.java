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
package org.apache.pulsar.client.impl.schema.generic;

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;


/**
 * Builder to build {@link org.apache.pulsar.client.api.schema.GenericRecord}.
 */
class AvroRecordBuilderImpl implements GenericRecordBuilder {

    private final GenericSchemaImpl genericSchema;
    private final org.apache.avro.generic.GenericRecordBuilder avroRecordBuilder;

    AvroRecordBuilderImpl(GenericSchemaImpl genericSchema) {
        this.genericSchema = genericSchema;
        this.avroRecordBuilder =
            new org.apache.avro.generic.GenericRecordBuilder(genericSchema.getAvroSchema());
    }

    /**
     * Sets the value of a field.
     *
     * @param fieldName the name of the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder set(String fieldName, Object value) {
        if (value instanceof GenericRecord) {
            if (value instanceof GenericAvroRecord) {
                avroRecordBuilder.set(fieldName, ((GenericAvroRecord) value).getAvroRecord());
            } else {
                throw new IllegalArgumentException("Avro Record Builder doesn't support non-avro record as a field");
            }
        } else {
            avroRecordBuilder.set(fieldName, value);
        }
        return this;
    }

    /**
     * Sets the value of a field.
     *
     * @param field the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder set(Field field, Object value) {
        set(field.getIndex(), value);
        return this;
    }

    /**
     * Sets the value of a field.
     *
     * @param index the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    protected GenericRecordBuilder set(int index, Object value) {
        if (value instanceof GenericRecord) {
            if (value instanceof GenericAvroRecord) {
                avroRecordBuilder.set(genericSchema.getAvroSchema().getFields().get(index),
                        ((GenericAvroRecord) value).getAvroRecord());
            } else {
                throw new IllegalArgumentException("Avro Record Builder doesn't support non-avro record as a field");
            }
        } else {
            avroRecordBuilder.set(
                    genericSchema.getAvroSchema().getFields().get(index),
                    value
            );
        }
        return this;
    }

    /**
     * Clears the value of the given field.
     *
     * @param fieldName the name of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder clear(String fieldName) {
        avroRecordBuilder.clear(fieldName);
        return this;
    }

    /**
     * Clears the value of the given field.
     *
     * @param field the field to clear.
     * @return a reference to the RecordBuilder.
     */
    @Override
    public GenericRecordBuilder clear(Field field) {
        return clear(field.getIndex());
    }

    /**
     * Clears the value of the given field.
     *
     * @param index the index of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    protected GenericRecordBuilder clear(int index) {
        avroRecordBuilder.clear(
            genericSchema.getAvroSchema().getFields().get(index));
        return this;
    }

    @Override
    public GenericRecord build() {
        return new GenericAvroRecord(
            null,
            genericSchema.getAvroSchema(),
            genericSchema.getFields(),
            avroRecordBuilder.build()
        );
    }
}
