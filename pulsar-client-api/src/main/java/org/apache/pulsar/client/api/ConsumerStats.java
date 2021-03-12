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
import java.util.Map;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Consumer statistics recorded by client.
 *
 * <p>All the stats are relative to the last recording period. The interval of the stats refreshes is configured with
 * {@link ClientBuilder#statsInterval(long, java.util.concurrent.TimeUnit)} with a default of 1 minute.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ConsumerStats extends Serializable {

    /**
     * @return Number of messages received in the last interval
     */
    long getNumMsgsReceived();

    /**
     * @return Number of bytes received in the last interval
     */
    long getNumBytesReceived();

    /**
     * @return Rate of bytes per second received in the last interval
     */
    double getRateMsgsReceived();

    /**
     * @return Rate of bytes per second received in the last interval
     */
    double getRateBytesReceived();

    /**
     * @return Number of message acknowledgments sent in the last interval
     */
    long getNumAcksSent();

    /**
     * @return Number of message acknowledgments failed in the last interval
     */
    long getNumAcksFailed();

    /**
     * @return Number of message receive failed in the last interval
     */
    long getNumReceiveFailed();

    /**
     * @return Number of message batch receive failed in the last interval
     */
    long getNumBatchReceiveFailed();

    /**
     * @return Total number of messages received by this consumer
     */
    long getTotalMsgsReceived();

    /**
     * @return Total number of bytes received by this consumer
     */
    long getTotalBytesReceived();

    /**
     * @return Total number of messages receive failures
     */
    long getTotalReceivedFailed();

    /**
     * @return Total number of messages batch receive failures
     */
    long getTotaBatchReceivedFailed();

    /**
     * @return Total number of message acknowledgments sent by this consumer
     */
    long getTotalAcksSent();

    /**
     * @return Total number of message acknowledgments failures on this consumer
     */
    long getTotalAcksFailed();

    /**
     * Get the size of receiver queue.
     * @return
     */
    Integer getMsgNumInReceiverQueue();

    /**
     * Get the receiver queue size of sub-consumers.
     * @return
     */
    Map<Long, Integer> getMsgNumInSubReceiverQueue();
}
