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
package org.apache.pulsar.broker.intercept;

import com.google.common.annotations.Beta;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Service that manages intercepting API calls to a pulsar cluster
 */
@Beta
public class InterceptService {
    private static final Logger log = LoggerFactory.getLogger(InterceptService.class);

    private InterceptProvider provider;
    private final ServiceConfiguration conf;
    private final TenantsInterceptService tenantInterceptService;
    private final NamespacesInterceptService namespaceInterceptService;
    private final TopicInterceptService topicInterceptService;
    private final FunctionsInterceptService functionInterceptService;
    private final SinksInterceptService sinkInterceptService;
    private final SourcesInterceptService sourceInterceptService;

    public InterceptService(ServiceConfiguration conf, PulsarAdmin pulsarAdmin)
            throws PulsarServerException {
        this.conf = conf;

        try {
            final String providerClassname = conf.getInterceptProvider();
            if (StringUtils.isNotBlank(providerClassname)) {
                provider = (InterceptProvider) Class.forName(providerClassname).newInstance();
                provider.initialize(conf, pulsarAdmin);
                log.info("Interceptor {} has been loaded.", providerClassname);
            } else {
                provider = new InterceptProvider() {};
            }

            tenantInterceptService = new TenantsInterceptService(provider.getTenantInterceptProvider());
            namespaceInterceptService = new NamespacesInterceptService(provider.getNamespaceInterceptProvider());
            topicInterceptService = new TopicInterceptService(provider.getTopicInterceptProvider());
            functionInterceptService = new FunctionsInterceptService(provider.getFunctionsInterceptProvider());
            sourceInterceptService = new SourcesInterceptService(provider.getSourcesInterceptProvider());
            sinkInterceptService = new SinksInterceptService(provider.getSinksInterceptProvider());

        } catch (Throwable e) {
            throw new PulsarServerException("Failed to load an intercept provider.", e);
        }
    }

    public TenantsInterceptService tenants() {
        return tenantInterceptService;
    }

    public NamespacesInterceptService namespaces() {
        return namespaceInterceptService;
    }

    public TopicInterceptService topics() {
        return topicInterceptService;
    }

    public FunctionsInterceptService functions() {
         return functionInterceptService;
    }

    public SourcesInterceptService sources() {
        return sourceInterceptService;
    }

    public SinksInterceptService sinks() {
        return sinkInterceptService;
    }
}
