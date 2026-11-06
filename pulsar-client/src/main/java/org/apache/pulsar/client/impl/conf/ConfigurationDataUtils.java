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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    /**
     * Like {@link #MAPPER}, but keeps null values. Used to read the default values of the properties.
     */
    private static final ObjectMapper DEFAULTS_MAPPER = create()
            .setDefaultPropertyInclusion(Include.ALWAYS);

    /**
     * Reads and writes the fields directly, bypassing the setters, and keeps null values. Used to put a property
     * back to the value a new configuration instance has.
     */
    private static final ObjectMapper FIELD_MAPPER = create()
            .setVisibility(PropertyAccessor.ALL, Visibility.NONE)
            .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
            .setDefaultPropertyInclusion(Include.ALWAYS);

    private static final List<String> AUTHENTICATION_KEYS = List.of("authPluginClassName", "authParams",
            "authParamMap");

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

    /**
     * Apply the properties in {@code config} to {@code target} through its setters, leaving every other property
     * as it is. Unlike {@link #loadData}, this keeps properties that are not serialized, such as listeners,
     * {@code @JsonIgnore} fields and {@code @Secret} values, which would otherwise be dropped or masked.
     *
     * <p>A property mapped to null goes back to its default value, as it does with {@link #loadData}.
     *
     * <p>{@code target} is modified in place, so callers that must not see a partial update on failure pass a
     * copy of their configuration.
     *
     * @return {@code target}
     */
    public static <T> T updateData(Map<String, Object> config, T target) {
        try {
            // The mapper skips null values
            T updated = MAPPER.updateValue(target, config);
            return resetNullProperties(config, updated);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config into existing configuration data", e);
        }
    }

    /**
     * With {@link #loadData}, a null value removes the property from the merged configuration, so the new instance
     * keeps its default value. Do the same here. The default is written to the field when it has the name of the
     * property, without going through the setter, which may reject it ({@code setProducerName}). A property whose
     * field is named differently, such as a boolean field {@code isReplProducer} for {@code replProducer}, is reset
     * through its setter. Like with {@link #loadData}, a null for a property that isn't serialized is ignored.
     */
    private static <T> T resetNullProperties(Map<String, Object> config, T target) throws IOException {
        JsonNode defaults = null;
        JsonNode fieldDefaults = null;
        ObjectNode fieldResets = null;
        ObjectNode setterResets = null;
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getValue() != null) {
                continue;
            }
            if (defaults == null) {
                Object newInstance = MAPPER.readValue("{}", target.getClass());
                defaults = DEFAULTS_MAPPER.valueToTree(newInstance);
                fieldDefaults = FIELD_MAPPER.valueToTree(newInstance);
                fieldResets = FIELD_MAPPER.createObjectNode();
                setterResets = MAPPER.createObjectNode();
            }
            String key = entry.getKey();
            if (!defaults.has(key)) {
                continue;
            }
            if (fieldDefaults.has(key)) {
                fieldResets.set(key, fieldDefaults.get(key));
            } else {
                setterResets.set(key, defaults.get(key));
            }
        }
        if (fieldResets == null) {
            return target;
        }
        target = FIELD_MAPPER.readerForUpdating(target).readValue(fieldResets);
        return MAPPER.readerForUpdating(target).readValue(setterResets);
    }

    /**
     * Whether {@code config} sets any of the properties the authentication is created from.
     */
    public static boolean containsAuthenticationKeys(Map<String, Object> config) {
        for (String key : AUTHENTICATION_KEYS) {
            if (config.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

}

