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
package org.apache.pulsar.websocket.admin;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.websocket.stats.ProxyTopicStat;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ConsumerStats;
import org.apache.pulsar.websocket.stats.ProxyTopicStat.ProducerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketProxyStatsBase extends WebSocketWebResource {
    private static final Logger LOG = LoggerFactory.getLogger(WebSocketProxyStatsBase.class);

    protected Collection<Metrics> internalGetMetrics() throws Exception {
        // Ensure super user access only
        validateSuperUserAccess();
        try {
            return service().getProxyStats().getMetrics();
        } catch (Exception e) {
            LOG.error("[{}] Failed to generate metrics", clientAppId(), e);
            throw new RestException(e);
        }
    }

    protected ProxyTopicStat internalGetStats(TopicName topicName) {
        validateUserAccess(topicName);
        ProxyTopicStat stats = getStat(topicName);
        if (stats == null) {
            throw new RestException(Status.NOT_FOUND, "Topic does not exist");
        }
        return stats;
    }

    protected Map<String, ProxyTopicStat> internalGetProxyStats() {
        validateSuperUserAccess();
        return getStat();
    }

    private ProxyTopicStat getStat(TopicName topicName) {
        String topicNameStr = topicName.toString();
        if (!service().getProducers().containsKey(topicNameStr) && !service().getConsumers().containsKey(topicNameStr)
                && !service().getReaders().containsKey(topicNameStr)) {
            LOG.warn("topic doesn't exist {}", topicNameStr);
            throw new RestException(Status.NOT_FOUND, "Topic does not exist");
        }
        ProxyTopicStat topicStat = new ProxyTopicStat();
        if (service().getProducers().containsKey(topicNameStr)) {
            service().getProducers().get(topicNameStr).forEach(handler -> {
                ProducerStats stat = new ProducerStats(handler);
                topicStat.producerStats.add(stat);

            });
        }

        if (service().getConsumers().containsKey(topicNameStr)) {
            service().getConsumers().get(topicNameStr).forEach(handler -> {
                topicStat.consumerStats.add(new ConsumerStats(handler));
            });
        }

        if (service().getReaders().containsKey(topicNameStr)) {
            service().getReaders().get(topicNameStr).forEach(handler -> {
                topicStat.consumerStats.add(new ConsumerStats(handler));
            });
        }
        return topicStat;
    }

    public Map<String, ProxyTopicStat> getStat() {

        Map<String, ProxyTopicStat> statMap = new HashMap<>();

        service().getProducers().forEach((topicName, handlers) -> {
            ProxyTopicStat topicStat = statMap.computeIfAbsent(topicName, t -> new ProxyTopicStat());
            handlers.forEach(handler -> topicStat.producerStats.add(new ProducerStats(handler)));
            statMap.put(topicName, topicStat);
        });
        service().getConsumers().forEach((topicName, handlers) -> {
            ProxyTopicStat topicStat = statMap.computeIfAbsent(topicName, t -> new ProxyTopicStat());
            handlers.forEach(handler -> topicStat.consumerStats.add(new ConsumerStats(handler)));
            statMap.put(topicName, topicStat);
        });
        service().getReaders().forEach((topicName, handlers) -> {
            ProxyTopicStat topicStat = statMap.computeIfAbsent(topicName, t -> new ProxyTopicStat());
            handlers.forEach(handler -> topicStat.consumerStats.add(new ConsumerStats(handler)));
            statMap.put(topicName, topicStat);
        });

        return statMap;
    }
}
