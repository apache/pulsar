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
package org.apache.pulsar.proxy.stats;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.text.SimpleDateFormat;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(value = { "DATE_FORMAT", "PRODUCER", "CONSUMER"})
public class ClientStats {
    String channelId;
    String type;
    long clientId;
    String producerName;
    String subscriptionName;
    String createTime;
    String topic;

    public static final String PRODUCER = "producer";
    public static final String CONSUMER = "consumer";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ClientStats(String channelId, String type, long clientId, String name,
                       String topic) {
        this.channelId = channelId;
        this.type = type;
        this.clientId = clientId;
        if (PRODUCER.equals(type)) {
            this.producerName = name;
        } else {
            this.subscriptionName = name;
        }
        this.createTime = DATE_FORMAT.format(new java.util.Date());
        this.topic = topic;
    }
}
