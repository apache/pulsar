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

package org.apache.pulsar.functions.utils;

import static org.apache.commons.lang.StringUtils.isEmpty;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.common.functions.CryptoConfig;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.functions.proto.Function;

public final class CryptoUtils {

    public static Function.CryptoSpec convert(CryptoConfig config) {
        Function.CryptoSpec.Builder bldr = Function.CryptoSpec.newBuilder()
                .setCryptoKeyReaderClassName(config.getCryptoKeyReaderClassName());

        if (config.getCryptoKeyReaderConfig() != null) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            String readerConfigString = new Gson().toJson(config.getCryptoKeyReaderConfig(), type);
            bldr.setCryptoKeyReaderConfig(readerConfigString);
        }

        if (config.getEncryptionKeys() != null && config.getEncryptionKeys().length > 0) {
            bldr.addAllProducerEncryptionKeyName(Arrays.asList(config.getEncryptionKeys()));
        }

        if (config.getProducerCryptoFailureAction() != null) {
            bldr.setProducerCryptoFailureAction(getProtoFailureAction(config.getProducerCryptoFailureAction()));
        }

        if (config.getConsumerCryptoFailureAction() != null) {
            bldr.setConsumerCryptoFailureAction(getProtoFailureAction(config.getConsumerCryptoFailureAction()));
        }

        return bldr.build();
    }

    public static CryptoConfig convertFromSpec(Function.CryptoSpec spec) {
        if (spec == null || isEmpty(spec.getCryptoKeyReaderClassName())) {
            return null;
        }

        CryptoConfig.CryptoConfigBuilder bldr = CryptoConfig.builder();

        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> cryptoReaderConfig = new Gson().fromJson(spec.getCryptoKeyReaderConfig(), type);

        bldr.cryptoKeyReaderClassName(spec.getCryptoKeyReaderClassName())
                .cryptoKeyReaderConfig(cryptoReaderConfig)
                .consumerCryptoFailureAction(getConsumerCryptoFailureAction(spec.getConsumerCryptoFailureAction()))
                .producerCryptoFailureAction(getProducerCryptoFailureAction(spec.getProducerCryptoFailureAction()))
                .encryptionKeys(spec.getProducerEncryptionKeyNameList().toArray(new String[0]));

        return bldr.build();
    }

    public static CryptoKeyReader getCryptoKeyReaderInstance(String className, Map<String, Object> configs, ClassLoader classLoader) {
        Class<?> cryptoClass;
        try {
            cryptoClass = ClassLoaderUtils.loadClass(className, classLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    String.format("Failed to load crypto key reader class %sx", className));
        }

        try {
            Constructor<?> ctor = cryptoClass.getConstructor(Map.class);
            return (CryptoKeyReader) ctor.newInstance(configs);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Key reader class does not have constructor accepts map", e);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Failed to create instance for key reader class", e);
        }
    }

    public static ProducerCryptoFailureAction getProducerCryptoFailureAction(Function.CryptoSpec.FailureAction action) {
        switch (action) {
            case FAIL:
                return ProducerCryptoFailureAction.FAIL;
            case SEND:
                return ProducerCryptoFailureAction.SEND;
            default:
                throw new RuntimeException("Unknown producer protobuf failure action " + action.getValueDescriptor().getName());
        }
    }

    public static ConsumerCryptoFailureAction getConsumerCryptoFailureAction(Function.CryptoSpec.FailureAction action) {
        switch (action) {
            case FAIL:
                return ConsumerCryptoFailureAction.FAIL;
            case DISCARD:
                return ConsumerCryptoFailureAction.DISCARD;
            case CONSUME:
                return ConsumerCryptoFailureAction.CONSUME;
            default:
                throw new RuntimeException("Unknown consumer protobuf failure action " + action.getValueDescriptor().getName());
        }
    }

    public static Function.CryptoSpec.FailureAction getProtoFailureAction(ProducerCryptoFailureAction action) {
        switch (action) {
            case FAIL:
                return Function.CryptoSpec.FailureAction.FAIL;
            case SEND:
                return Function.CryptoSpec.FailureAction.SEND;
            default:
                throw new RuntimeException("Unknown producer crypto failure action " + action);
        }
    }

    public static Function.CryptoSpec.FailureAction getProtoFailureAction(ConsumerCryptoFailureAction action) {
        switch (action) {
            case FAIL:
                return Function.CryptoSpec.FailureAction.FAIL;
            case DISCARD:
                return Function.CryptoSpec.FailureAction.DISCARD;
            case CONSUME:
                return Function.CryptoSpec.FailureAction.CONSUME;
            default:
                throw new RuntimeException("Unknown consumer crypto failure action " + action);
        }
    }

}
