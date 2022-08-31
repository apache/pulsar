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

import static org.apache.commons.lang.StringUtils.isBlank;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Slf4j
public class IOConfigUtils {
    public static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz, SourceContext sourceContext) {
        return loadWithSecrets(map, clazz, secretName -> sourceContext.getSecret(secretName));
    }

    public static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz, SinkContext sinkContext) {
        return loadWithSecrets(map, clazz, secretName -> sinkContext.getSecret(secretName));
    }

    public static Map<String, Object> loadConfigFromJsonString(String config) throws JsonProcessingException {
        if (!isBlank(config)) {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(config, new TypeReference<Map<String, Object>>() {
            });
        } else {
            return Collections.emptyMap();
        }
    }

    private static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz,
                                         Function<String, String> secretsGetter) {
        Map<String, Object> configs = new HashMap<>(map);

        for (Field field : Reflections.getAllFields(clazz)) {
            field.setAccessible(true);
            for (Annotation annotation : field.getAnnotations()) {
                if (annotation.annotationType().equals(FieldDoc.class)) {
                    FieldDoc fieldDoc = (FieldDoc) annotation;
                    if (fieldDoc.sensitive()) {
                        String secret;
                        try {
                            secret = secretsGetter.apply(field.getName());
                        } catch (Exception e) {
                            log.warn("Failed to read secret {}", field.getName(), e);
                            break;
                        }
                        if (secret != null) {
                            configs.put(field.getName(), secret);
                        }
                    }
                    configs.computeIfAbsent(field.getName(), key -> {
                        if (fieldDoc.required()) {
                            throw new IllegalArgumentException(field.getName() + " cannot be null");
                        }
                        String value = fieldDoc.defaultValue();
                        if (!StringUtils.isEmpty(value)) {
                            return value;
                        }
                        return null;
                    });
                }
            }
        }
        return new ObjectMapper().convertValue(configs, clazz);
    }
}
