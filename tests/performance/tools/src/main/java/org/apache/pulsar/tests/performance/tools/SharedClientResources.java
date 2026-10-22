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
package org.apache.pulsar.tests.performance.tools;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientSharedResources;

final class SharedClientResources {
    /**
     * The resources that a workload's clients share, sized by its I/O and listener threads.
     *
     * <p>The clients' memory isn't limited: a shared memory limit controller without a configured limit has none. The
     * gateways' maxOutstanding bounds the messages that they have in flight, and each producer keeps the client's
     * default limits of pending messages. A consumer uses the memory limit only when it auto-scales its receiver queue,
     * which the applications' pods don't.
     */
    static PulsarClientSharedResources create(int ioThreads, int listenerThreads) {
        return PulsarClientSharedResources.builder()
                .configureEventLoop(config -> config.numberOfThreads(ioThreads))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ListenerExecutor,
                        config -> config.numberOfThreads(listenerThreads))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.InternalExecutor,
                        config -> config.numberOfThreads(ioThreads))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ScheduledExecutor,
                        config -> config.numberOfThreads(Math.min(2, ioThreads)))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.LookupExecutor,
                        config -> config.numberOfThreads(Math.min(2, ioThreads)))
                .build();
    }

    /**
     * A builder of a client of the scenario's cluster that uses the shared resources, trusts the scenario's CA
     * certificate when it has one, and authenticates with its token when it has one.
     */
    static ClientBuilder clientBuilder(IotScenario scenario, PulsarClientSharedResources sharedResources) {
        ClientBuilder builder = PulsarClient.builder()
                .serviceUrl(scenario.serviceUrl())
                .sharedResources(sharedResources);
        if (scenario.tlsTrustCertsFilePath() != null) {
            builder.tlsTrustCertsFilePath(scenario.tlsTrustCertsFilePath());
        }
        if (scenario.authTokenFilePath() != null) {
            // Reads the token from the file whenever the client authenticates, as a client configured with a token
            // file does
            Path tokenFile = Path.of(scenario.authTokenFilePath());
            builder.authentication(AuthenticationFactory.token(() -> readToken(tokenFile)));
        }
        return builder;
    }

    private static String readToken(Path tokenFile) {
        try {
            return Files.readString(tokenFile).trim();
        } catch (IOException e) {
            throw new UncheckedIOException("Couldn't read the token " + tokenFile, e);
        }
    }

    private SharedClientResources() {
    }
}
