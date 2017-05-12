package com.yahoo.pulsar.websocket.stats;

import java.util.Set;

import com.google.common.collect.Sets;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.websocket.ConsumerHandler;
import com.yahoo.pulsar.websocket.ProducerHandler;

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
            this.remoteConnection = handler.getRemote().getInetSocketAddress().toString();
            this.numberOfMsgDelivered = handler.getMsgDeliveredCounter();
        }

        public String remoteConnection;
        public String subscriptionName;
        public SubscriptionType subscriptionType;
        public long numberOfMsgDelivered;
    }

}
