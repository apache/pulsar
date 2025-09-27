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

import static org.apache.pulsar.client.impl.PulsarClientResourcesConfigurer.createDnsResolverGroupWithResourceConfig;
import static org.apache.pulsar.client.impl.PulsarClientResourcesConfigurer.createEventLoopGroupWithResourceConfig;
import static org.apache.pulsar.client.impl.PulsarClientResourcesConfigurer.createExternalExecutorProviderWithResourceConfig;
import static org.apache.pulsar.client.impl.PulsarClientResourcesConfigurer.createInternalExecutorProviderWithResourceConfig;
import static org.apache.pulsar.client.impl.PulsarClientResourcesConfigurer.createLookupExecutorProviderWithResourceConfig;
import static org.apache.pulsar.client.impl.PulsarClientResourcesConfigurer.createScheduledExecutorProviderWithResourceConfig;
import static org.apache.pulsar.client.impl.PulsarClientResourcesConfigurer.createTimer;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

@Getter
public class PulsarClientSharedResourcesImpl implements PulsarClientSharedResources {
    Set<SharedResource> sharedResources;
    protected final EventLoopGroup ioEventLoopGroup;
    private final ExecutorProvider internalExecutorProvider;
    private final ExecutorProvider externalExecutorProvider;
    private final ScheduledExecutorProvider scheduledExecutorProvider;
    private final Timer timer;
    private final ExecutorProvider lookupExecutorProvider;
    private final DnsResolverGroupImpl dnsResolverGroup;

    public PulsarClientSharedResourcesImpl(Set<SharedResource> sharedResources,
                                           Map<SharedResource, PulsarClientSharedResourcesBuilderImpl.ResourceConfig>
                                                   resourceConfigs) {
        if (sharedResources.isEmpty()) {
            this.sharedResources = EnumSet.allOf(SharedResource.class);
        } else {
            this.sharedResources = Set.copyOf(sharedResources);
        }
        if (this.sharedResources.contains(SharedResource.DnsResolver)
            && !this.sharedResources.contains(SharedResource.EventLoopGroup)) {
            throw new IllegalArgumentException(
                    "It's necessary to enable ResourceType.eventLoopGroup when using ResourceType.dnsResolverGroup");
        }
        this.ioEventLoopGroup = this.sharedResources.contains(SharedResource.EventLoopGroup)
                ? createEventLoopGroupWithResourceConfig(
                getResourceConfig(resourceConfigs, SharedResource.EventLoopGroup))
                : null;
        this.externalExecutorProvider = this.sharedResources.contains(SharedResource.ListenerExecutor)
                ? createExternalExecutorProviderWithResourceConfig(
                getResourceConfig(resourceConfigs, SharedResource.ListenerExecutor))
                : null;
        this.internalExecutorProvider = this.sharedResources.contains(SharedResource.InternalExecutor)
                ? createInternalExecutorProviderWithResourceConfig(
                getResourceConfig(resourceConfigs, SharedResource.InternalExecutor))
                : null;
        this.scheduledExecutorProvider = this.sharedResources.contains(SharedResource.ScheduledExecutor)
                ? createScheduledExecutorProviderWithResourceConfig(
                getResourceConfig(resourceConfigs, SharedResource.ScheduledExecutor))
                : null;
        this.lookupExecutorProvider = this.sharedResources.contains(SharedResource.LookupExecutor)
                ? createLookupExecutorProviderWithResourceConfig(
                getResourceConfig(resourceConfigs, SharedResource.LookupExecutor))
                : null;
        this.timer = this.sharedResources.contains(SharedResource.Timer)
                ? createTimer(getResourceConfig(resourceConfigs, SharedResource.Timer))
                : null;
        this.dnsResolverGroup = this.sharedResources.contains(SharedResource.DnsResolver)
                ? createDnsResolverGroupWithResourceConfig(
                        getResourceConfig(resourceConfigs, SharedResource.DnsResolver),
                        ioEventLoopGroup)
                : null;
    }

    private <T extends PulsarClientSharedResourcesBuilderImpl.ResourceConfig> T getResourceConfig(
            Map<SharedResource, PulsarClientSharedResourcesBuilderImpl.ResourceConfig> resourceConfigs,
            SharedResource resource) {
        return (T) resourceConfigs.get(resource);
    }


    @Override
    public boolean contains(SharedResource sharedResource) {
        return sharedResources.contains(sharedResource);
    }

    public Collection<SharedResource> getSharedResources() {
        return sharedResources;
    }

    @Override
    public void close() throws PulsarClientException {
        if (externalExecutorProvider != null) {
            externalExecutorProvider.shutdownNow();
        }
        if (internalExecutorProvider != null) {
            internalExecutorProvider.shutdownNow();
        }
        if (scheduledExecutorProvider != null) {
            scheduledExecutorProvider.shutdownNow();
        }
        if (lookupExecutorProvider != null) {
            lookupExecutorProvider.shutdownNow();
        }
        if (dnsResolverGroup != null) {
            dnsResolverGroup.close();
        }
        if (timer != null) {
            timer.stop();
        }
        if (ioEventLoopGroup != null) {
            EventLoopUtil.shutdownGracefully(ioEventLoopGroup);
        }
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
