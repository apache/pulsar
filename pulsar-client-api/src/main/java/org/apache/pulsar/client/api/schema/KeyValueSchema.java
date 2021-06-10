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

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

/**
 * This interface models a Schema that is composed of two parts.
 * A Key and a Value.
 * @param <K> the type of the Key
 * @param <V> the type of the Value.
 */
public interface KeyValueSchema<K,V> extends Schema<KeyValue<K,V>> {

    /**
     * Get the Schema of the Key.
     * @return the Schema of the Key
     */
    Schema<K> getKeySchema();

    /**
     * Get the Schema of the Value.
     *
     * @return the Schema of the Value
     */
    Schema<V> getValueSchema();

    /**
     * Get the KeyValueEncodingType.
     *
     * @return the KeyValueEncodingType
     * @see KeyValueEncodingType#INLINE
     * @see KeyValueEncodingType#SEPARATED
     */
    KeyValueEncodingType getKeyValueEncodingType();
}
