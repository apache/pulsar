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
package org.apache.pulsar.metadata.bookkeeper;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.EmbeddedServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration2.ex.ConfigurationException;

/**
 * Builds bookies that run inside a broker's JVM (the {@code -rb} broker option and standalone).
 *
 * <p>The broker's /metrics reads the Prometheus Java client 1.x default registry. A bookie in the same process
 * must not register into it: its JVM metrics would be a second copy of the broker's and the bookie metrics
 * belong to the bookie's own stats endpoint. A provider that can take a registry, like Pulsar's, therefore
 * gets one of its own, and the bookie is built around that provider instead of the one {@code EmbeddedServer}
 * would otherwise create from the same configuration through the no-arg constructor.
 */
public final class BookieStatsProviders {

    private BookieStatsProviders() {
    }

    /**
     * Creates the stats provider named by {@code statsProviderClass} in the configuration, with a private
     * {@link PrometheusRegistry} when the provider accepts one. The caller owns the provider: start it before
     * the bookie built with {@link #buildBookieServer} and stop it after that bookie is closed.
     */
    public static StatsProvider create(ServerConfiguration conf) throws ConfigurationException {
        Class<? extends StatsProvider> statsProviderClass = conf.getStatsProviderClass();
        try {
            return statsProviderClass.getConstructor(PrometheusRegistry.class).newInstance(new PrometheusRegistry());
        } catch (NoSuchMethodException e) {
            return ReflectionUtils.newInstance(statsProviderClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Could not create the bookie stats provider " + statsProviderClass.getName(),
                    e);
        }
    }

    /**
     * Builds a bookie around {@code statsProvider}. {@code EmbeddedServer} leaves an injected provider out of
     * the returned component stack, so starting and stopping it stays with the caller.
     */
    public static LifecycleComponentStack buildBookieServer(ServerConfiguration conf, StatsProvider statsProvider)
            throws Exception {
        return EmbeddedServer.builder(new BookieConfiguration(conf))
                .statsProvider(statsProvider)
                .build()
                .getLifecycleComponentStack();
    }
}
