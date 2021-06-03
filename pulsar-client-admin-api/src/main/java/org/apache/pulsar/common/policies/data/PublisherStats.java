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
package org.apache.pulsar.common.policies.data;

import java.util.Map;
import org.apache.pulsar.client.api.ProducerAccessMode;

/**
 * Statistics about a publisher.
 */
public interface PublisherStats {

    ProducerAccessMode getAccessMode();

    /** Total rate of messages published by this publisher (msg/s). */
    double getMsgRateIn();

    /** Total throughput of messages published by this publisher (byte/s). */
    double getMsgThroughputIn();

    /** Average message size published by this publisher. */
    double getAverageMsgSize();

    /** total chunked message count received. **/
    double getChunkedMessageRate();

    /** Id of this publisher. */
    long getProducerId();

    /** Producer name. */
    String getProducerName();

    /** Address of this publisher. */
    String getAddress();

    /** Timestamp of connection. */
    String getConnectedSince();

    /** Client library version. */
    String getClientVersion();

    /** Metadata (key/value strings) associated with this publisher. */
    Map<String, String> getMetadata();
}
