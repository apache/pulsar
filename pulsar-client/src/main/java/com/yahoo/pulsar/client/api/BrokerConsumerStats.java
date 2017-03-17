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
package com.yahoo.pulsar.client.api;

public interface BrokerConsumerStats {
    /*
     * @return - Whether the stats are valid. Call the {@link Consumer.getBrokerConsumerStatsAsync()} again if the
     * function returns false.
     */
    public boolean isValid();

    /*
     * @return - Rate at which the messages are delivered to the consumer. msg/s
     */
    public double getMsgRateOut();

    /*
     * @return - Throughput at which the messages are delivered to the consumer. bytes/s
     */
    public double getMsgThroughputOut();

    /*
     * @return - Rate at which the messages are redelivered by this consumer. msg/s
     */
    public double getMsgRateRedeliver();

    /*
     * @return - Name of the consumer.
     */
    public String getConsumerName();

    /*
     * @return - Number of available message permits for the consumer
     */
    public long getAvailablePermits();

    /*
     * @return - Number of unacknowledged messages for the consumer
     */
    public long getUnackedMessages();

    /*
     * @return - Flag to verify if consumer is blocked due to reaching threshold of unacked messages
     */
    public boolean isBlockedConsumerOnUnackedMsgs();

    /*
     * @return - Address of this consumer
     */
    public String getAddress();

    /*
     * @return - Timestamp of the connection
     */
    public String getConnectedSince();

    /*
     * @return - Whether this subscription is Exclusive or Shared or Failover
     */
    public SubscriptionType getSubscriptionType();

    /*
     * @return - Rate at which the messages are delivered to the consumer. msg/s
     */
    public double getMsgRateExpired();

    /*
     * @return - Number of messages in the subscription backlog
     */
    public long getMsgBacklog();
}
