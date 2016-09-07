/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.stats;

import java.util.Map;

import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;

import com.google.common.collect.Maps;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;

/**
 */
public class BookieClientStatsGenerator {
    private final PulsarService pulsar;

    // map<namespace, map<destination, bookieOpsStats>>
    private Map<String, Map<String, PendingBookieOpsStats>> nsBookieClientStatsMap;

    public BookieClientStatsGenerator(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.nsBookieClientStatsMap = Maps.newTreeMap();
    }

    public static Map<String, Map<String, PendingBookieOpsStats>> generate(PulsarService pulsar) throws Exception {
        return new BookieClientStatsGenerator(pulsar).generate();
    }

    private Map<String, Map<String, PendingBookieOpsStats>> generate() throws Exception {
        if (pulsar.getBrokerService() != null && pulsar.getBrokerService().getTopics() != null) {
            pulsar.getBrokerService().getTopics().forEach((name, topicFuture) -> {
                PersistentTopic persistentTopic = (PersistentTopic) topicFuture.getNow(null);
                if (persistentTopic != null) {
                    DestinationName destinationName = DestinationName.get(persistentTopic.getName());
                    put(destinationName, persistentTopic.getManagedLedger().getStats().getPendingBookieOpsStats());
                }
            });
        }

        return nsBookieClientStatsMap;
    }

    private void put(DestinationName destinationName, PendingBookieOpsStats bookieOpsStats) {
        String namespace = destinationName.getNamespace();
        if (!nsBookieClientStatsMap.containsKey(namespace)) {
            Map<String, PendingBookieOpsStats> destBookieClientStatsMap = Maps.newTreeMap();
            destBookieClientStatsMap.put(destinationName.toString(), bookieOpsStats);
            nsBookieClientStatsMap.put(namespace, destBookieClientStatsMap);
        } else {
            nsBookieClientStatsMap.get(namespace).put(destinationName.toString(), bookieOpsStats);
        }

    }
}
