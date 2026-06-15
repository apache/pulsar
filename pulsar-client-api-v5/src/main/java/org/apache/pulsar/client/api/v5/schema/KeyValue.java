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
package org.apache.pulsar.client.api.v5.schema;

/**
 * A key/value pair, used as the {@linkplain GenericRecord#nativeObject() native object} of a
 * {@link GenericRecord} whose {@linkplain GenericRecord#schemaType() schema type} is
 * {@link SchemaType#KEY_VALUE}. Each side may itself be a {@link GenericRecord} (for a structured
 * key or value), a primitive, or {@code null}.
 *
 * @param key   the key (may be {@code null})
 * @param value the value (may be {@code null})
 * @param <K>   the key type
 * @param <V>   the value type
 */
public record KeyValue<K, V>(K key, V value) {
}
