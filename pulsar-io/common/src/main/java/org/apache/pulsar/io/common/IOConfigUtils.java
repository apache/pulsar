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
package org.apache.pulsar.io.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Slf4j
public class IOConfigUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz, SourceContext sourceContext) {
        return loadWithSecrets(map, clazz, secretName -> sourceContext.getSecret(secretName));
    }

    public static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz, SinkContext sinkContext) {
        return loadWithSecrets(map, clazz, secretName -> sinkContext.getSecret(secretName));
    }

    public static Map<String, Object> loadConfigFromJsonString(String config) throws JsonProcessingException {
        if (config == null || config.isEmpty()) {
            return Collections.emptyMap();
        }
        return MAPPER.readValue(config, new TypeReference<>() {});
    }

    private static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz,
                                         Function<String, String> secretsGetter) {
        Map<String, Object> configs = new HashMap<>(map);

        for (Field field : getAllFields(clazz)) {
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
                        // Use default value if it is not null before checking required
                        String value = fieldDoc.defaultValue();
                        if (value != null && !value.isEmpty()) {
                            return value;
                        }
                        if (fieldDoc.required()) {
                            throw new IllegalArgumentException(field.getName() + " cannot be null");
                        }
                        return null;
                    });
                }
            }
        }
        return MAPPER.convertValue(configs, clazz);
    }

    private static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new LinkedList<>();
        fields.addAll(Arrays.asList(type.getDeclaredFields()));
        if (type.getSuperclass() != null) {
            fields.addAll(getAllFields(type.getSuperclass()));
        }
        return fields;
    }
}
