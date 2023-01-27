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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import java.io.IOException;
import java.util.Collection;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamicSkipUnknownPropertyHandler extends DeserializationProblemHandler {

    @Getter
    @Setter
    private boolean skipUnknownProperty = true;

    @Override
    public boolean handleUnknownProperty(DeserializationContext deserializationContext, JsonParser p,
                                         JsonDeserializer<?> deserializer, Object beanOrClass,
                                         String propertyName) throws IOException {
        Collection<Object> propIds = (deserializer == null) ? null : deserializer.getKnownPropertyNames();
        UnrecognizedPropertyException unrecognizedPropertyException = UnrecognizedPropertyException
                .from(p, beanOrClass, propertyName, propIds);
        if (skipUnknownProperty){
            if (log.isDebugEnabled()) {
                log.debug(unrecognizedPropertyException.getMessage());
            }
            p.skipChildren();
            return skipUnknownProperty;
        } else {
            throw unrecognizedPropertyException;
        }
    }
}
