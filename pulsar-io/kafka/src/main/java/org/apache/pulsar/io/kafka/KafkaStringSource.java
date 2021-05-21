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

package org.apache.pulsar.io.kafka;

import org.apache.pulsar.client.api.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.nio.charset.StandardCharsets;

/**
 * Simple Kafka Source that just transfers the value part of the kafka records
 * as Strings
 */
public class KafkaStringSource extends KafkaAbstractSource<String> {


    @Override
    public KafkaRecord buildRecord(ConsumerRecord<Object, Object> consumerRecord) {
        KafkaRecord record = new KafkaRecord(consumerRecord,
                new String((byte[]) consumerRecord.value(), StandardCharsets.UTF_8),
                Schema.STRING);
        return record;
    }

}
