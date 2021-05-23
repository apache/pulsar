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
package org.apache.pulsar.client.admin.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleAbstractTypeResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Provides custom configuration for jackson.
 */
@Provider
public class JacksonConfigurator implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    public JacksonConfigurator() {
        mapper = ObjectMapperFactory.create();
        setInterfaceDefaultMapperModule();
    }

    private void setInterfaceDefaultMapperModule() {
        SimpleModule module = new SimpleModule("InterfaceDefaultMapperModule");

        // we not specific @JsonDeserialize annotation in OffloadProcessStatus
        // because we do not want to have jackson dependency in pulsar-client-admin-api
        // In this case we use SimpleAbstractTypeResolver to map interfaces to impls
        SimpleAbstractTypeResolver resolver = new SimpleAbstractTypeResolver();
        resolver.addMapping(OffloadProcessStatus.class, OffloadProcessStatusImpl.class);

        module.setAbstractTypes(resolver);
        mapper.registerModule(module);
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }

}
