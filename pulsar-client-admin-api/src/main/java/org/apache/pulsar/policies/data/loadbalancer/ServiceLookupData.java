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

import java.util.Map;
import java.util.Optional;

/**
 * For backwards compatibility purposes.
 */
public interface ServiceLookupData {
    String getWebServiceUrl();

    String getWebServiceUrlTls();

    String getPulsarServiceUrl();

    String getPulsarServiceUrlTls();

    /**
     * Get all the protocols advertised by the broker.
     *
     * @return the protocols advertised by the broker.
     */
    Map<String, String> getProtocols();

    /**
     * Get the protocol data of the given <tt>protocol</tt>.
     *
     * @param protocol the protocol advertised by the broker.
     * @return the optional protocol data advertised by the broker.
     */
    Optional<String> getProtocol(String protocol);

}
