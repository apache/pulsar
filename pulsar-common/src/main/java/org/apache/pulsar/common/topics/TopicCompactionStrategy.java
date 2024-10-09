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
package org.apache.pulsar.common.topics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Defines a custom strategy to compact messages in a topic.
 * This strategy can be passed to Topic Compactor and Table View to compact messages in a custom way.
 *
 * Examples:
 *
 * TopicCompactionStrategy strategy = new MyTopicCompactionStrategy();
 *
 * // Run topic compaction by the compaction strategy.
 * // While compacting messages for each key,
 * //   it will choose messages only if TopicCompactionStrategy.shouldKeepLeft(prev, cur) returns false.
 * StrategicTwoPhaseCompactor compactor = new StrategicTwoPhaseCompactor(...);
 * compactor.compact(topic, strategy);
 *
 * // Run table view by the compaction strategy.
 * // While updating messages in the table view <key,value> map,
 * //   it will choose messages only if TopicCompactionStrategy.shouldKeepLeft(prev, cur) returns false.
 * TableView tableView = pulsar.getClient().newTableViewBuilder(strategy.getSchema())
 *                 .topic(topic)
 *                 .loadConf(Map.of(
 *                         "topicCompactionStrategyClassName", strategy.getClass().getCanonicalName()))
 *                 .create();
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface TopicCompactionStrategy<T> {

    String TABLE_VIEW_TAG = "table-view";
    Map<String, TopicCompactionStrategy> INSTANCES = new ConcurrentHashMap<>();

    /**
     * Returns the schema object for this strategy.
     * @return
     */
    Schema<T> getSchema();
    /**
     * Tests if the compaction needs to keep the left(previous message)
     * compared to the right(current message) for the same key.
     *
     * @param prev previous message value
     * @param cur current message value
     * @return True if it needs to keep the previous message and ignore the current message. Otherwise, False.
     */
    boolean shouldKeepLeft(T prev, T cur);

    default void handleSkippedMessage(String key, T cur) {
    }


    static TopicCompactionStrategy load(String tag, String topicCompactionStrategyClassName) {
        if (topicCompactionStrategyClassName == null) {
            return null;
        }

        try {
            Class<?> clazz = Class.forName(topicCompactionStrategyClassName);
            TopicCompactionStrategy instance = (TopicCompactionStrategy) clazz.getDeclaredConstructor().newInstance();
            INSTANCES.put(tag, instance);
            return instance;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Error when loading topic compaction strategy: " + topicCompactionStrategyClassName, e);
        }
    }

    static TopicCompactionStrategy getInstance(String tag) {
        return INSTANCES.get(tag);
    }
}
