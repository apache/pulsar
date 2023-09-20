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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Provider
public class JsonMapperProvider implements ContextResolver<ObjectMapper> {
    private final ObjectMapper mapper = ObjectMapperFactory.create();

    public JsonMapperProvider(){

    }

    public JsonMapperProvider(DeserializationProblemHandler handler){
        mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.addHandler(handler);
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}