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
package org.apache.pulsar.functions.utils;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Map;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.common.functions.MessagePayloadProcessorConfig;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.functions.proto.Function;

public class MessagePayloadProcessorUtils {
    public static MessagePayloadProcessor getMessagePayloadProcessorInstance(String className,
                                                                             Map<String, Object> configs,
                                                                             ClassLoader classLoader) {
        Class<?> payloadProcessorClass;
        try {
            payloadProcessorClass = ClassLoaderUtils.loadClass(className, classLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    String.format("Failed to load message payload processor class %sx", className));
        }

        try {
            if (configs == null || configs.isEmpty()) {
                Constructor<?> ctor = payloadProcessorClass.getConstructor();
                return (MessagePayloadProcessor) ctor.newInstance();
            } else {
                Constructor<?> ctor = payloadProcessorClass.getConstructor(Map.class);
                return (MessagePayloadProcessor) ctor.newInstance(configs);
            }
        } catch (NoSuchMethodException e) {
            if (configs == null || configs.isEmpty()) {
                throw new RuntimeException("Message payload processor class does not have default constructor", e);
            } else {
                throw new RuntimeException("Message payload processor class does not have constructor accepts map", e);
            }
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Failed to create instance for message payload processor class", e);
        }
    }

    public static MessagePayloadProcessorConfig convertFromSpec(Function.MessagePayloadProcessorSpec spec) {
        if (spec == null || isEmpty(spec.getClassName())) {
            return null;
        }

        MessagePayloadProcessorConfig.MessagePayloadProcessorConfigBuilder bldr =
                MessagePayloadProcessorConfig.builder();

        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> configs = new Gson().fromJson(spec.getConfigs(), type);

        bldr.className(spec.getClassName()).config(configs);

        return bldr.build();
    }

    public static Function.MessagePayloadProcessorSpec convert(MessagePayloadProcessorConfig config) {
        Function.MessagePayloadProcessorSpec.Builder bldr = Function.MessagePayloadProcessorSpec.newBuilder()
                .setClassName(config.getClassName());

        if (config.getConfig() != null) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            String readerConfigString = new Gson().toJson(config.getConfig(), type);
            bldr.setConfigs(readerConfigString);
        }

        return bldr.build();
    }
}
