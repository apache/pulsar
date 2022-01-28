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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Protcol type to determine type of proxy routing when client connects to proxy using
 * {@link ClientBuilder::proxyServiceUrl}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum ProxyProtocol {
    /**
     * Follows SNI-routing
     * https://docs.trafficserver.apache.org/en/latest/admin-guide/layer-4-routing.en.html#sni-routing.
     **/
    SNI
}
