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

package org.apache.pulsar.io.cassandra;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

/**
 * Cassandra sink that treats incoming messages on the input topic as Strings
 * and write identical key/value pairs.
 */
@Connector(
    name = "cassandra",
    type = IOType.SINK,
    help = "The CassandraStringSink is used for moving messages from Pulsar to Cassandra.",
    configClass = CassandraSinkConfig.class)
public class CassandraStringSink extends CassandraAbstractSink<String, String> {
    @Override
    public KeyValue<String, String> extractKeyValue(Record<byte[]> record) {
        String key = record.getKey().orElseGet(() -> new String(record.getValue()));
        return new KeyValue<>(key, new String(record.getValue()));
    }
}