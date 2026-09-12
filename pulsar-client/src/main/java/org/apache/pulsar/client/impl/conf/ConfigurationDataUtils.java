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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Utils for loading configuration data.
 */
public final class ConfigurationDataUtils {

    /**
     * PIP-337 SSL-factory client configuration keys removed outright in Pulsar 5.0 (PIP-478). A stale value in
     * a {@code loadConf} map is rejected with an actionable migration message pointing to the
     * {@code tlsFactoryClassName} successor, rather than the generic "unrecognized field" error.
     */
    static final List<String> REMOVED_PIP337_TLS_FACTORY_KEYS =
            List.of("sslFactoryPlugin", "sslFactoryPluginParams");

    /**
     * The old PIP-337 default {@code sslFactoryPlugin} implementation FQCN. Its class is deleted in Pulsar 5.0
     * (PIP-478), so this is a plain string literal. A {@code *Plugin} key still carrying this default value is
     * equivalent to "unset" (no custom factory), so it is tolerated — only a non-default (custom) factory needs
     * migration.
     */
    static final String DEFAULT_PIP337_SSL_FACTORY_CLASS = "org.apache.pulsar.common.util.DefaultPulsarSslFactory";

    /**
     * Reject a stale, non-default PIP-337 SSL-factory key (removed in Pulsar 5.0, PIP-478) left in a client
     * {@code loadConf} map. A key that is absent or blank is tolerated; a {@code *Plugin} key still naming the
     * old default factory FQCN ({@link #DEFAULT_PIP337_SSL_FACTORY_CLASS}) is also tolerated (equivalent to
     * unset); any other non-blank value fails loudly with an actionable migration message.
     *
     * @param config the client configuration map
     * @throws IllegalArgumentException if a removed PIP-337 key is present with a non-default value
     */
    public static void rejectRemovedPip337TlsFactoryKeys(Map<String, Object> config) {
        if (config == null) {
            return;
        }
        for (String key : REMOVED_PIP337_TLS_FACTORY_KEYS) {
            Object value = config.get(key);
            if (value == null || StringUtils.isBlank(value.toString())) {
                continue;
            }
            // A *Plugin key still set to the old default factory FQCN means no custom factory — tolerate it as
            // if unset. The *PluginParams keys carry no default value, so any non-blank value is rejected.
            if (key.endsWith("Plugin") && DEFAULT_PIP337_SSL_FACTORY_CLASS.equals(value.toString().trim())) {
                continue;
            }
            throw new IllegalArgumentException("The PIP-337 '" + key + "' client configuration key is "
                    + "removed in Pulsar 5.0 (PIP-478). Migrate the custom SSL factory to a PulsarTlsFactory "
                    + "selected by tlsFactoryClassName / tlsFactoryConfig and remove '" + key + "' from the "
                    + "configuration.");
        }
    }

    public static ObjectMapper create() {
        ObjectMapper mapper = ObjectMapperFactory.create();
        // forward compatibility for the properties may go away in the future
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, false);
        mapper.setDefaultPropertyInclusion(Include.NON_NULL);
        return mapper;
    }

    private static final ObjectMapper MAPPER = create();

    private ConfigurationDataUtils() {}

    @SuppressWarnings("unchecked")
    public static <T> T loadData(Map<String, Object> config,
                                 T existingData,
                                 Class<T> dataCls) {
        try {
            String existingConfigJson = MAPPER.writeValueAsString(existingData);
            Map<String, Object> existingConfig = MAPPER.readValue(existingConfigJson, Map.class);
            Map<String, Object> newConfig = new HashMap<>();
            newConfig.putAll(existingConfig);
            newConfig.putAll(config);
            String configJson = MAPPER.writeValueAsString(newConfig);
            return MAPPER.readValue(configJson, dataCls);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config into existing configuration data", e);
        }

    }

}

