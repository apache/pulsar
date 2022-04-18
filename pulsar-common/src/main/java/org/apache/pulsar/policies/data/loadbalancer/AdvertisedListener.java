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
package org.apache.pulsar.policies.data.loadbalancer;

import java.net.URI;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The advertisedListener for broker with brokerServiceUrl and brokerServiceUrlTls.
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AdvertisedListener {
    //
    // the broker service uri without ssl
    private URI brokerServiceUrl;
    //
    // the broker service uri with ssl
    private URI brokerServiceUrlTls;

    //
    // the broker service uri without ssl
    private URI brokerHttpUrl;
    //
    // the broker service uri with ssl
    private URI brokerHttpsUrl;

    public boolean hasUriForProtocol(String protocol) {
        if ("pulsar".equals(protocol)) {
            return brokerServiceUrl != null;
        } else if ("pulsar+ssl".equals(protocol)) {
            return brokerServiceUrlTls != null;
        } else if ("http".equals(protocol)) {
            return brokerHttpUrl != null;
        } else if ("https".equals(protocol)) {
            return brokerHttpsUrl != null;
        } else {
            return false;
        }
    }
}
