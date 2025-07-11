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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<InetSocketAddress, EndpointStatus> hostAvailabilityMap = new ConcurrentHashMap<>();
    private final long serviceUrlQuarantineInitDurationMs;
    private final long serviceUrlQuarantineMaxDurationMs;
    private final boolean enableServiceUrlQuarantine;

    public PulsarServiceNameResolver() {
        this(0, 0);
    }

    public PulsarServiceNameResolver(long serviceUrlQuarantineInitDurationMs, long serviceUrlQuarantineMaxDurationMs) {
        this.serviceUrlQuarantineInitDurationMs = serviceUrlQuarantineInitDurationMs;
        this.serviceUrlQuarantineMaxDurationMs = serviceUrlQuarantineMaxDurationMs;
        this.enableServiceUrlQuarantine =
                serviceUrlQuarantineInitDurationMs > 0 && serviceUrlQuarantineMaxDurationMs > 0;
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
            if (availableAddressList != null) {
                log.warn("No available hosts found for service url: {}", serviceUrl);
            }
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
        this.allAddressSet = Set.copyOf(addresses);
        this.serviceUrl = serviceUrl;
        this.serviceUri = uri;
        this.currentIndex = randomIndex(addresses.size());
        if (enableServiceUrlQuarantine) {
            hostAvailabilityMap.keySet().retainAll(allAddressSet);
            allAddressSet.forEach(
                    address -> hostAvailabilityMap.putIfAbsent(address, createEndpointStatus(true, address)));
            availableAddressList = new ArrayList<>(hostAvailabilityMap.keySet());
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
        if (!enableServiceUrlQuarantine) {
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
                computeEndpointStatus(false, endpointStatus);
                if (!availableHostsChanged.get() && endpointStatus.isAvailable()) {
                    availableHostsChanged.set(true);
                }
            }
        });

        if (availableHostsChanged.get()) {
            availableAddressList = hostAvailabilityMap.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().isAvailable() && allAddressSet.contains(entry.getKey()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            log.info("service name resolver available hosts changed, current available hosts: {}",
                    availableAddressList);
        }
    }

    @VisibleForTesting
    List<InetSocketAddress> getAvailableAddressList() {
        return availableAddressList;
    }

    /**
     * Create an {@link EndpointStatus} for the given address.
     * @param isAvailable the availability status of the endpoint
     * @param inetSocketAddress the address of the endpoint
     * @return a new {@link EndpointStatus} instance
     */
    private EndpointStatus createEndpointStatus(boolean isAvailable, InetSocketAddress inetSocketAddress) {
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(serviceUrlQuarantineInitDurationMs, TimeUnit.MILLISECONDS)
                .setMax(serviceUrlQuarantineMaxDurationMs, TimeUnit.MILLISECONDS)
                .create();
        EndpointStatus endpointStatus =
                new EndpointStatus(inetSocketAddress, backoff, System.currentTimeMillis(), 0,
                        isAvailable);
        if (!isAvailable) {
            computeEndpointStatus(false, endpointStatus);
        }
        return endpointStatus;
    }

    /**
     * Updates the endpoint's availability status based on the given input flag and internal quarantine logic.
     *
     * <p>This method applies the input flag directly, and includes a time-based self-healing mechanism: if the
     * endpoint has been marked unavailable for a sufficient cooldown period (quarantine), it automatically transitions
     * back to available even when {@code newIsAvailable} is {@code false}.
     *
     * <p>This allows the system to retry endpoints that were previously marked as unavailable.
     * If the endpoint fails again after recovery, it is marked unavailable and re-enters quarantine
     * with an exponentially increased delay before the next recovery attempt.
     *
     * <p>The backoff is only reset when a successful update occurs after recovery
     * (i.e., when {@code newIsAvailable} is {@code true} and the endpoint was previously marked unavailable).
     *
     * @param newIsAvailable the new availability status of the endpoint
     * @param status         the current status of the endpoint
     */
    private void computeEndpointStatus(boolean newIsAvailable, EndpointStatus status) {
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
                    status.setNextDelayMsToRecover(status.getQuarantineBackoff().next());
                }
            } else {
                // from available to unavailable
                status.setAvailable(false);
                status.setLastUpdateTimeStampMs(System.currentTimeMillis());
                status.setNextDelayMsToRecover(status.getQuarantineBackoff().next());
            }
        } else if (!status.isAvailable()) {
            // from unavailable to available
            status.setAvailable(true);
            status.setLastUpdateTimeStampMs(System.currentTimeMillis());
            status.setNextDelayMsToRecover(0);
            status.getQuarantineBackoff().reset();
        }
    }
}
