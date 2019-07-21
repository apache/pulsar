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
package org.apache.pulsar.client.impl;

import java.net.InetSocketAddress;
import java.net.URI;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.common.net.ServiceURI;

/**
 * A service name resolver to resolve real socket address.
 */
public interface ServiceNameResolver {

    /**
     * Resolve pulsar service url.
     *
     * @return resolve the service url to return a socket address
     */
    InetSocketAddress resolveHost();

    /**
     * Resolve pulsar service url
     * @return
     */
    URI resolveHostUri();

    /**
     * Get service url.
     *
     * @return service url
     */
    String getServiceUrl();

    /**
     * Get service uri.
     *
     * @return service uri
     */
    ServiceURI getServiceUri();

    /**
     * Update service url.
     *
     * @param serviceUrl service url
     */
    void updateServiceUrl(String serviceUrl) throws InvalidServiceURL;

}
