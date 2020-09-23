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
package org.apache.pulsar.websocket.stats;

import java.util.Set;

import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.websocket.ConsumerHandler;
import org.apache.pulsar.websocket.ProducerHandler;
import org.apache.pulsar.websocket.ReaderHandler;

import com.google.common.collect.Sets;

/**
 * Stats of topic served by web-socket proxy. It shows current stats of producers and consumers connected to proxy for a
 * given topic
 */
public class ProxyTopicStat {

    public final Set<ProducerStats> producerStats;
    public final Set<ConsumerStats> consumerStats;

    public ProxyTopicStat() {
        this.producerStats = Sets.newHashSet();
        this.consumerStats = Sets.newHashSet();
    }

    public static class ProducerStats {
        public ProducerStats() {
        }

        public ProducerStats(ProducerHandler handler) {
            this.remoteConnection = handler.getRemote().getInetSocketAddress().toString();
            this.numberOfMsgPublished = handler.getMsgPublishedCounter();
        }

        public String remoteConnection;
        public long numberOfMsgPublished;
    }

    public static class ConsumerStats {

        public ConsumerStats() {
        }

        public ConsumerStats(ConsumerHandler handler) {
            this.subscriptionName = handler.getSubscription();
            this.subscriptionType = handler.getSubscriptionType();
            this.subscriptionMode = handler.getSubscriptionMode();
            this.remoteConnection = handler.getRemote().getInetSocketAddress().toString();
            this.numberOfMsgDelivered = handler.getMsgDeliveredCounter();
        }
        
        public ConsumerStats(ReaderHandler handler) {
            this.subscriptionName = handler.getSubscription();
            this.subscriptionType = handler.getSubscriptionType();
            this.remoteConnection = handler.getRemote().getInetSocketAddress().toString();
            this.numberOfMsgDelivered = handler.getMsgDeliveredCounter();
        }

        public String remoteConnection;
        public String subscriptionName;
        public SubscriptionType subscriptionType;
        public SubscriptionMode subscriptionMode;
        public long numberOfMsgDelivered;
    }

}
