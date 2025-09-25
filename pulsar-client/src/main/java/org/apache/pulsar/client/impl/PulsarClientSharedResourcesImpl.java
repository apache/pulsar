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

import static org.apache.pulsar.client.impl.PulsarClientImpl.createExternalExecutorProvider;
import static org.apache.pulsar.client.impl.PulsarClientImpl.createInternalExecutorProvider;
import static org.apache.pulsar.client.impl.PulsarClientImpl.createLookupExecutorProvider;
import static org.apache.pulsar.client.impl.PulsarClientImpl.createScheduledExecutorProvider;
import static org.apache.pulsar.client.impl.PulsarClientImpl.createTimer;
import static org.apache.pulsar.client.impl.PulsarClientImpl.getEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

@Getter
public class PulsarClientSharedResourcesImpl implements PulsarClientSharedResources {
    Set<ResourceType> resourceTypes;
    protected final EventLoopGroup ioEventLoopGroup;
    private final ExecutorProvider internalExecutorProvider;
    private final ExecutorProvider externalExecutorProvider;
    private final ScheduledExecutorProvider scheduledExecutorProvider;
    private final Timer timer;
    private final ExecutorProvider lookupExecutorProvider;
    private final DnsResolverGroupImpl dnsResolverGroup;

    public PulsarClientSharedResourcesImpl(Set<ResourceType> resourceTypes,
                                           ClientConfigurationData conf) {
        if (resourceTypes.isEmpty()) {
            this.resourceTypes = EnumSet.allOf(ResourceType.class);
        } else {
            this.resourceTypes = Set.copyOf(resourceTypes);
        }
        if (this.resourceTypes.contains(ResourceType.dnsResolver)
            && !this.resourceTypes.contains(ResourceType.eventLoopGroup)) {
            throw new IllegalArgumentException(
                    "It's necessary to enable ResourceType.eventLoopGroup when using ResourceType.dnsResolverGroup");
        }
        this.ioEventLoopGroup = this.resourceTypes.contains(ResourceType.eventLoopGroup)
                ? getEventLoopGroup(conf)
                : null;
        this.externalExecutorProvider = this.resourceTypes.contains(ResourceType.externalExecutor)
                ? createExternalExecutorProvider(conf)
                : null;
        this.internalExecutorProvider = this.resourceTypes.contains(ResourceType.internalExecutor)
                ? createInternalExecutorProvider(conf)
                : null;
        this.scheduledExecutorProvider = this.resourceTypes.contains(ResourceType.scheduledExecutor)
                ? createScheduledExecutorProvider(conf)
                : null;
        this.lookupExecutorProvider = this.resourceTypes.contains(ResourceType.lookupExecutor)
                ? createLookupExecutorProvider()
                : null;
        this.timer = this.resourceTypes.contains(ResourceType.timer)
                ? createTimer()
                : null;
        this.dnsResolverGroup = this.resourceTypes.contains(ResourceType.dnsResolver)
                ? new DnsResolverGroupImpl(Objects.requireNonNull(ioEventLoopGroup), conf)
                : null;
    }

    @Override
    public boolean contains(ResourceType resourceType) {
        return resourceTypes.contains(resourceType);
    }

    @Override
    public Collection<ResourceType> getResourceTypes() {
        return resourceTypes;
    }

    @Override
    public void close() throws PulsarClientException {
        externalExecutorProvider.shutdownNow();
        internalExecutorProvider.shutdownNow();
        scheduledExecutorProvider.shutdownNow();
        lookupExecutorProvider.shutdownNow();
        dnsResolverGroup.close();
        timer.stop();
        EventLoopUtil.shutdownGracefully(ioEventLoopGroup);
    }

    public void applyTo(PulsarClientImpl.PulsarClientImplBuilder instanceBuilder) {
        if (externalExecutorProvider != null) {
            instanceBuilder.externalExecutorProvider(externalExecutorProvider);
        }
        if (internalExecutorProvider != null) {
            instanceBuilder.internalExecutorProvider(internalExecutorProvider);
        }
        if (scheduledExecutorProvider != null) {
            instanceBuilder.scheduledExecutorProvider(scheduledExecutorProvider);
        }
        if (lookupExecutorProvider != null) {
            instanceBuilder.lookupExecutorProvider(lookupExecutorProvider);
        }
        if (dnsResolverGroup != null) {
            instanceBuilder.dnsResolverGroup(dnsResolverGroup);
        }
        if (timer != null) {
            instanceBuilder.timer(timer);
        }
        if (ioEventLoopGroup != null) {
            instanceBuilder.eventLoopGroup(ioEventLoopGroup);
        }
    }
}
