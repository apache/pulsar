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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.common.net.ServiceURI;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.BackoffBuilder;

/**
 * The default implementation of {@link ServiceNameResolver}.
 */
@Slf4j
public class PulsarServiceNameResolver implements ServiceNameResolver {

    private volatile ServiceURI serviceUri;
    private volatile String serviceUrl;
    private static final AtomicIntegerFieldUpdater<PulsarServiceNameResolver> CURRENT_INDEX_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PulsarServiceNameResolver.class, "currentIndex");
    private volatile int currentIndex;
    private volatile List<InetSocketAddress> allAddressList;
    private volatile Set<InetSocketAddress> allAddressSet;
    private volatile List<InetSocketAddress> availableAddressList;
    private final Map<InetSocketAddress, EndpointStatus> hostAvailabilityMap = new HashMap<>();
    private final long serviceUrlRecoveryInitBackoffIntervalMs;
    private final long serviceUrlRecoveryMaxBackoffIntervalMs;
    private final boolean enableServiceUrlBackoffRecovery;

    public PulsarServiceNameResolver() {
        this.serviceUrlRecoveryInitBackoffIntervalMs = 0;
        this.serviceUrlRecoveryMaxBackoffIntervalMs = 0;
        this.enableServiceUrlBackoffRecovery = false;
    }

    public PulsarServiceNameResolver(long initialBackoffTimeMs, long maxBackoffTimeMs) {
        this.serviceUrlRecoveryInitBackoffIntervalMs = initialBackoffTimeMs;
        this.serviceUrlRecoveryMaxBackoffIntervalMs = maxBackoffTimeMs;
        this.enableServiceUrlBackoffRecovery = initialBackoffTimeMs > 0 && maxBackoffTimeMs > 0;
    }

    @Override
    public InetSocketAddress resolveHost() {
        final List<InetSocketAddress> list;
        List<InetSocketAddress> availableAddresses = availableAddressList;
        if (availableAddresses != null && !availableAddresses.isEmpty()) {
            list = availableAddresses;
        } else {
            // if no available address, use the original address list
            list = allAddressList;
        }
        checkState(
            list != null, "No service url is provided yet");
        checkState(
            !list.isEmpty(), "No hosts found for service url : " + serviceUrl);
        if (list.size() == 1) {
            return list.get(0);
        } else {
            int originalIndex = CURRENT_INDEX_UPDATER.getAndUpdate(this, last -> (last + 1) % list.size());
            return list.get((originalIndex + 1) % list.size());
        }
    }

    @Override
    public URI resolveHostUri() {
        InetSocketAddress host = resolveHost();
        String hostUrl = serviceUri.getServiceScheme() + "://" + host.getHostString() + ":" + host.getPort();
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
    public synchronized void updateServiceUrl(String serviceUrl) throws InvalidServiceURL {
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
                addresses.add(InetSocketAddress.createUnresolved(hostUri.getHost(), hostUri.getPort()));
            } catch (URISyntaxException e) {
                log.error("Invalid host provided {}", hostUrl, e);
                throw new InvalidServiceURL(e);
            }
        }
        this.allAddressList = addresses;
        this.allAddressSet = new HashSet<>(addresses);
        this.serviceUrl = serviceUrl;
        this.serviceUri = uri;
        this.currentIndex = randomIndex(addresses.size());
        if (enableServiceUrlBackoffRecovery) {
            this.availableAddressList = new ArrayList<>(addresses);
            hostAvailabilityMap.keySet().removeIf(host -> !allAddressSet.contains(host));
            addresses.forEach(address -> hostAvailabilityMap.putIfAbsent(address, createEndpointStatus(true, address)));
        }
    }

    private static int randomIndex(int numAddresses) {
        return numAddresses == 1
                ?
                0 : io.netty.util.internal.PlatformDependent.threadLocalRandom().nextInt(numAddresses);
    }

    /**
     * The method is executed under a synchronized lock and cannot execute code that may block, such as network io.
     * @param address the host address to mark availability for
     * @param isAvailable true if the host is available, false otherwise
     */
    @Override
    public synchronized void markHostAvailability(InetSocketAddress address, boolean isAvailable) {
        if (!enableServiceUrlBackoffRecovery) {
            return;
        }

        if (!allAddressSet.contains(address)) {
            // If the address is not part of the original service URL, we ignore it.
            log.debug("Address {} is not part of the original service URL, ignoring availability update", address);
            return;
        }

        AtomicBoolean availableHostsChanged = new AtomicBoolean(false);
        hostAvailabilityMap.compute(address, (key, oldStatus) -> {
            if (oldStatus == null) {
                EndpointStatus endpointStatus = createEndpointStatus(isAvailable, key);
                availableHostsChanged.set(true);
                return endpointStatus;
            }
            if (oldStatus.isAvailable() != isAvailable) {
                availableHostsChanged.set(true);
            }
            computeEndpointStatus(isAvailable, oldStatus);
            return oldStatus;
        });

        hostAvailabilityMap.forEach((__, endpointStatus) -> {
            if (!endpointStatus.isAvailable()) {
                boolean changed = computeEndpointStatus(false, endpointStatus);
                if (!availableHostsChanged.get()) {
                    availableHostsChanged.set(changed);
                }
            }
        });

        if (availableHostsChanged.get()) {
            availableAddressList = hostAvailabilityMap.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().isAvailable())
                    .filter(entry -> allAddressSet.contains(entry.getKey()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            log.info("service name resolver available hosts changed, current available hosts: {}",
                    availableAddressList);
        }
    }

    /**
     * Create an {@link EndpointStatus} for the given address.
     * @param isAvailable the availability status of the endpoint
     * @param inetSocketAddress the address of the endpoint
     * @return a new {@link EndpointStatus} instance
     */
    private EndpointStatus createEndpointStatus(boolean isAvailable, InetSocketAddress inetSocketAddress) {
        Backoff recoverBackoff = new BackoffBuilder()
                .setInitialTime(serviceUrlRecoveryInitBackoffIntervalMs, TimeUnit.MILLISECONDS)
                .setMax(serviceUrlRecoveryMaxBackoffIntervalMs, TimeUnit.MILLISECONDS)
                .create();
        EndpointStatus endpointStatus =
                new EndpointStatus(inetSocketAddress, recoverBackoff, System.currentTimeMillis(), 0,
                        isAvailable);
        if (!isAvailable) {
            endpointStatus.setNextDelayMsToRecover(endpointStatus.getRecoverBackoff().next());
        }
        return endpointStatus;
    }

    /**
     * Compute the endpoint status based on the new availability status.
     * @param newIsAvailable the new availability status of the endpoint
     * @param status the current status of the endpoint
     * @return true if the availability status has changed, false otherwise
     */
    private boolean computeEndpointStatus(boolean newIsAvailable, EndpointStatus status) {
        if (!newIsAvailable) {
            if (!status.isAvailable()) {
                // from unavailable to unavailable, check if we need to try to recover
                long elapsedTimeMsSinceLast = System.currentTimeMillis() - status.getLastUpdateTimeStampMs();
                boolean needTryRecover = elapsedTimeMsSinceLast >= status.getNextDelayMsToRecover();
                if (needTryRecover) {
                    log.info("service name resolver try to recover host {} after {}", status.getSocketAddress(),
                            Duration.ofMillis(elapsedTimeMsSinceLast));
                    status.setAvailable(true);
                    status.setLastUpdateTimeStampMs(System.currentTimeMillis());
                    status.setNextDelayMsToRecover(status.getRecoverBackoff().next());
                }
            } else {
                // from available to unavailable
                status.setAvailable(false);
                status.setLastUpdateTimeStampMs(System.currentTimeMillis());
                status.setNextDelayMsToRecover(status.getRecoverBackoff().next());
            }
        } else if (!status.isAvailable()) {
            // from unavailable to available
            status.setAvailable(true);
            status.setLastUpdateTimeStampMs(System.currentTimeMillis());
            status.setNextDelayMsToRecover(0);
            status.getRecoverBackoff().reset();
        }

        return newIsAvailable != status.isAvailable();
    }
}
