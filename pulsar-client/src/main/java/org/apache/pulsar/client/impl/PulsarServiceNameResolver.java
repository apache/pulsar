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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.annotations.VisibleForTesting;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.common.net.ServiceURI;

/**
 * The default implementation of {@link ServiceNameResolver}.
 */
@Slf4j
public class PulsarServiceNameResolver implements ServiceNameResolver {

    private volatile ServiceURI serviceUri;
    private volatile String serviceUrl;
    private volatile List<InetSocketAddress> addressList;

    private final Predicate<InetSocketAddress> addressPredicate;

    public PulsarServiceNameResolver() {
        this(address -> !address.isUnresolved());
    }

    @VisibleForTesting
    public PulsarServiceNameResolver(Predicate<InetSocketAddress> addressPredicate) {
        this.addressPredicate = addressPredicate;
    }


    @Override
    public InetSocketAddress resolveHost() {
        final List<InetSocketAddress> list = addressList;
        checkState(list != null, "No service url is provided yet");
        checkState(!list.isEmpty(), "No hosts found for service url : " + serviceUrl);

        Collections.shuffle(list);
        for (InetSocketAddress candidate: list) {
            if (addressPredicate.test(candidate)) {
                return candidate;
            }
        }

        throw new IllegalStateException("No host is reachable for service url: " + serviceUrl);
    }

    @Override
    public URI resolveHostUri() {
        InetSocketAddress host = resolveHost();
        String hostUrl = serviceUri.getServiceScheme() + "://" + host.getHostName() + ":" + host.getPort();
        return URI.create(hostUrl);
    }

    @Override
    public String getServiceUrl() {
        return serviceUrl;
    }

    @Override
    public ServiceURI getServiceUri() {
        return serviceUri;
    }

    @Override
    public void updateServiceUrl(String serviceUrl) throws InvalidServiceURL {
        ServiceURI uri;
        try {
            uri = ServiceURI.create(serviceUrl);
        } catch (IllegalArgumentException iae) {
            log.error("Invalid service-url {} provided {}", serviceUrl, iae.getMessage(), iae);
            throw new InvalidServiceURL(iae);
        }

        String[] hosts = uri.getServiceHosts();
        List<InetSocketAddress> addresses = new ArrayList<>(hosts.length);
        for (String host : hosts) {
            String hostUrl = uri.getServiceScheme() + "://" + host;
            try {
                URI hostUri = new URI(hostUrl);
                addresses.add(new InetSocketAddress(hostUri.getHost(), hostUri.getPort()));
            } catch (URISyntaxException e) {
                log.error("Invalid host provided {}", hostUrl, e);
                throw new InvalidServiceURL(e);
            }
        }
        this.addressList = addresses;
        this.serviceUrl = serviceUrl;
        this.serviceUri = uri;
    }
}
