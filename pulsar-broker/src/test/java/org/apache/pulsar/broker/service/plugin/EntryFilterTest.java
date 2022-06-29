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


import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.common.api.proto.KeyValue;

@Slf4j
public class EntryFilterTest implements EntryFilter {
    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        if (context.getMsgMetadata() == null || context.getMsgMetadata().getPropertiesCount() <= 0) {
            return FilterResult.ACCEPT;
        }
        Consumer consumer = context.getConsumer();
        Map<String, String> metadata = consumer.getMetadata();
        log.info("filterEntry for {}", metadata);
        String matchValueAccept = metadata.getOrDefault("matchValueAccept", "ACCEPT");
        String matchValueReject = metadata.getOrDefault("matchValueReject", "REJECT");
        String matchValueReschedule = metadata.getOrDefault("matchValueReschedule", "RESCHEDULE");
        List<KeyValue> list = context.getMsgMetadata().getPropertiesList();
        // filter by string
        for (KeyValue keyValue : list) {
            if (matchValueAccept.equalsIgnoreCase(keyValue.getKey())) {
                log.info("metadata {} key {} outcome ACCEPT", metadata, keyValue.getKey());
                return FilterResult.ACCEPT;
            } else if (matchValueReject.equalsIgnoreCase(keyValue.getKey())){
                log.info("metadata {} key {} outcome REJECT", metadata, keyValue.getKey());
                return FilterResult.REJECT;
            } else if (matchValueReschedule.equalsIgnoreCase(keyValue.getKey())){
                log.info("metadata {} key {} outcome RESCHEDULE", metadata, keyValue.getKey());
                return FilterResult.RESCHEDULE;
            } else {
                log.info("metadata {} key {} outcome ??", metadata, keyValue.getKey());
            }
        }
        return null;
    }

    @Override
    public void close() {

    }
}
