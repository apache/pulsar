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
package org.apache.pulsar.client.api;

import java.io.Serializable;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Producer statistics recorded by client.
 *
 * <p>All the stats are relative to the last recording period. The interval of the stats refreshes is configured with
 * {@link ClientBuilder#statsInterval(long, java.util.concurrent.TimeUnit)} with a default of 1 minute.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ProducerStats extends Serializable {
    /**
     * @return the number of messages published in the last interval
     */
    long getNumMsgsSent();

    /**
     * @return the number of bytes sent in the last interval
     */
    long getNumBytesSent();

    /**
     * @return the number of failed send operations in the last interval
     */
    long getNumSendFailed();

    /**
     * @return the number of send acknowledges received by broker in the last interval
     */
    long getNumAcksReceived();

    /**
     * @return the messages send rate in the last interval
     */
    double getSendMsgsRate();

    /**
     * @return the bytes send rate in the last interval
     */
    double getSendBytesRate();

    /**
     * @return the 50th percentile of the send latency in milliseconds for the last interval
     */
    double getSendLatencyMillis50pct();

    /**
     * @return the 75th percentile of the send latency in milliseconds for the last interval
     */
    double getSendLatencyMillis75pct();

    /**
     * @return the 95th percentile of the send latency in milliseconds for the last interval
     */
    double getSendLatencyMillis95pct();

    /**
     * @return the 99th percentile of the send latency in milliseconds for the last interval
     */
    double getSendLatencyMillis99pct();

    /**
     * @return the 99.9th percentile of the send latency in milliseconds for the last interval
     */
    double getSendLatencyMillis999pct();

    /**
     * @return the max send latency in milliseconds for the last interval
     */
    double getSendLatencyMillisMax();

    /**
     * @return the total number of messages published by this producer
     */
    long getTotalMsgsSent();

    /**
     * @return the total number of bytes sent by this producer
     */
    long getTotalBytesSent();

    /**
     * @return the total number of failed send operations
     */
    long getTotalSendFailed();

    /**
     * @return the total number of send acknowledges received by broker
     */
    long getTotalAcksReceived();

}
