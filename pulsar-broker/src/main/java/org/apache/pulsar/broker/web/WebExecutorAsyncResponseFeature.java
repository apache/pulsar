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
package org.apache.pulsar.broker.web;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Feature;
import jakarta.ws.rs.core.FeatureContext;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.glassfish.jersey.InjectionManagerProvider;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.internal.inject.InjectionManager;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;

/**
 * Makes the {@link AsyncResponse} injected for {@code @Suspended} parameters complete the request on the web service
 * executor, see {@link WebExecutorAsyncResponse}.
 *
 * <p>It registers a {@link ValueParamProvider} with a higher priority than Jersey's own provider for
 * {@code @Suspended} parameters, and wraps the {@link AsyncResponse} that Jersey's provider supplies.
 */
public class WebExecutorAsyncResponseFeature implements Feature {

    private final WebExecutorThreadPool executor;

    public WebExecutorAsyncResponseFeature(WebExecutorThreadPool executor) {
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public boolean configure(FeatureContext context) {
        InjectionManager injectionManager = InjectionManagerProvider.getInjectionManager(context);
        context.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new AsyncResponseValueParamProvider(injectionManager, executor)).to(ValueParamProvider.class);
            }
        });
        return true;
    }

    private static final class AsyncResponseValueParamProvider implements ValueParamProvider {

        private final InjectionManager injectionManager;
        private final WebExecutorThreadPool executor;

        AsyncResponseValueParamProvider(InjectionManager injectionManager, WebExecutorThreadPool executor) {
            this.injectionManager = injectionManager;
            this.executor = executor;
        }

        @Override
        public Function<ContainerRequest, ?> getValueProvider(Parameter parameter) {
            // Jersey's provider also supplies subtypes of AsyncResponse, such as its AsyncContext, which the
            // wrapper doesn't implement, so leave those parameters to it
            if (parameter.getSource() != Parameter.Source.SUSPENDED
                    || !parameter.getRawType().isAssignableFrom(WebExecutorAsyncResponse.class)) {
                return null;
            }
            // Delegate to the provider that would be used without this one, which is Jersey's own provider
            List<ValueParamProvider> providers = injectionManager.getAllInstances(ValueParamProvider.class);
            Function<ContainerRequest, ?> delegateProvider = providers.stream()
                    .filter(provider -> provider != this)
                    .sorted(Comparator.comparingInt(
                            (ValueParamProvider provider) -> provider.getPriority().getWeight()).reversed())
                    .map(provider -> provider.getValueProvider(parameter))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
            if (delegateProvider == null) {
                return null;
            }
            return request -> {
                Object asyncResponse = delegateProvider.apply(request);
                return asyncResponse instanceof AsyncResponse
                        ? new WebExecutorAsyncResponse((AsyncResponse) asyncResponse, executor)
                        : asyncResponse;
            };
        }

        @Override
        public PriorityType getPriority() {
            return Priority.HIGH;
        }
    }
}
