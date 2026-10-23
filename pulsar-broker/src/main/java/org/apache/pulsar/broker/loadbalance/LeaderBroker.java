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
package org.apache.pulsar.broker.loadbalance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A class to hold the contents of the leader election node. Facilitates serialization and deserialization of the
 * information that might be added for leader broker in the future.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LeaderBroker {
    private String brokerId;
    private String serviceUrl;

    public String getBrokerId() {
        if (brokerId != null) {
            return brokerId;
        } else {
            // for backward compatibility at runtime with older versions of Pulsar
            return parseHostAndPort(serviceUrl);
        }
    }

    private static String parseHostAndPort(String serviceUrl) {
        int uriSeparatorPos = serviceUrl.indexOf("://");
        if (uriSeparatorPos == -1) {
            throw new IllegalArgumentException("'" + serviceUrl + "' isn't an URI.");
        }
        return serviceUrl.substring(uriSeparatorPos + 3);
    }
}
