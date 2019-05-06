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
package org.apache.pulsar.io.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class IOConfigUtils {
    public static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz, SourceContext sourceContext) {
        Map<String, Object> configs = new HashMap<>(map);

        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            for (Annotation annotation : field.getAnnotations()) {
                if (annotation.annotationType().equals(FieldDoc.class)) {

                    if (((FieldDoc) annotation).sensitive()) {
                        String secret = null;
                        try {
                            secret = sourceContext.getSecret(field.getName());
                        } catch (Exception e) {
                            log.warn("Failed to read secret {}", field.getName(), e);
                            break;
                        }

                        if (secret != null) {
                            configs.put(field.getName(), secret);
                        }
                    }
                }

            }
        }
        return ObjectMapperFactory.getThreadLocal().convertValue(configs, clazz);
    }
}
