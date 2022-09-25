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
package org.apache.pulsar.broker.service.plugin;


import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Consumer;

@Slf4j
public class EntryFilterProducerTest implements EntryFilter {
    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        if (context.getMsgMetadata() == null) {
            return FilterResult.ACCEPT;
        }
        Consumer consumer = context.getConsumer();
        Map<String, String> metadata = consumer != null ? consumer.getMetadata() : Collections.emptyMap();
        log.info("filterEntry for {}", metadata);
        String matchValueAccept = metadata.getOrDefault("matchValueAccept", "ACCEPT");
        String matchValueReject = metadata.getOrDefault("matchValueReject", "REJECT");
        String matchValueReschedule = metadata.getOrDefault("matchValueReschedule", "RESCHEDULE");
        // filter by string
        String producerName = context.getMsgMetadata().getProducerName();
        if (matchValueAccept.equalsIgnoreCase(producerName)) {
            log.info("metadata {} producerName {} outcome ACCEPT", metadata, producerName);
            return FilterResult.ACCEPT;
        } else if (matchValueReject.equalsIgnoreCase(producerName)){
            log.info("metadata {} producerName {} outcome REJECT", metadata, producerName);
            return FilterResult.REJECT;
        } else if (matchValueReschedule.equalsIgnoreCase(producerName)){
            log.info("metadata {} producerName {} outcome RESCHEDULE", metadata, producerName);
            return FilterResult.RESCHEDULE;
        } else {
            log.info("metadata {} producerName {} outcome ??", metadata, producerName);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
