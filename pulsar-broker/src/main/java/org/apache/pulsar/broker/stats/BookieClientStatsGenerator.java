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
package org.apache.pulsar.broker.stats;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

/**
 */
public class BookieClientStatsGenerator {
    private final PulsarService pulsar;

    // map<namespace, map<topic, bookieOpsStats>>
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

            pulsar.getBrokerService().forEachTopic(topic -> {
                if (topic instanceof PersistentTopic) {
                    PersistentTopic persistentTopic = (PersistentTopic) topic;
                    TopicName topicName = TopicName.get(persistentTopic.getName());
                    put(topicName, persistentTopic.getManagedLedger().getStats().getPendingBookieOpsStats());
                }
            });
        }

        return nsBookieClientStatsMap;
    }

    private void put(TopicName topicName, PendingBookieOpsStats bookieOpsStats) {
        String namespace = topicName.getNamespace();
        if (!nsBookieClientStatsMap.containsKey(namespace)) {
            Map<String, PendingBookieOpsStats> destBookieClientStatsMap = Maps.newTreeMap();
            destBookieClientStatsMap.put(topicName.toString(), bookieOpsStats);
            nsBookieClientStatsMap.put(namespace, destBookieClientStatsMap);
        } else {
            nsBookieClientStatsMap.get(namespace).put(topicName.toString(), bookieOpsStats);
        }

    }
}
