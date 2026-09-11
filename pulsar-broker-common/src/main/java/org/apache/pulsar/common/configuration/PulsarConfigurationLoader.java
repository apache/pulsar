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
package org.apache.pulsar.common.configuration;

import static java.util.Objects.requireNonNull;
import static org.apache.pulsar.common.util.FieldParser.update;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * Loads ServiceConfiguration with properties.
 *
 *
 */
@CustomLog
public class PulsarConfigurationLoader {

    /**
     * PIP-337 TLS-factory configuration keys removed outright in Pulsar 5.0 (PIP-478). A stale value left in a
     * config file (broker.conf / proxy.conf) is rejected loudly at load with an actionable migration message —
     * preserving the fail-loud detection the removed {@code @Deprecated} fields used to provide, without
     * retaining a functional PIP-337 field.
     */
    static final List<String> REMOVED_PIP337_TLS_FACTORY_KEYS = List.of(
            "sslFactoryPlugin", "sslFactoryPluginParams",
            "brokerClientSslFactoryPlugin", "brokerClientSslFactoryPluginParams",
            // broker.conf / proxy.conf pass a brokerClient_<clientKey> property through to the internal client
            // (PropertiesUtils.filterAndMapProperties strips the "brokerClient_" prefix), where a stale client
            // sslFactoryPlugin would otherwise be silently dropped by ClientConfigurationData ignoreUnknown.
            // Reject that prefixed form too so the removed client key cannot bypass the fail-loud detection.
            "brokerClient_sslFactoryPlugin", "brokerClient_sslFactoryPluginParams");

    /**
     * The old PIP-337 default {@code sslFactoryPlugin} implementation FQCN. Its class is deleted in Pulsar 5.0
     * (PIP-478), so this is a plain string literal. A {@code *Plugin} key still carrying this default value is
     * equivalent to "unset" (no custom factory), so it is tolerated — only a non-default (custom) factory needs
     * migration.
     */
    static final String DEFAULT_PIP337_SSL_FACTORY_CLASS = "org.apache.pulsar.common.util.DefaultPulsarSslFactory";

    /**
     * Reject a stale, non-default PIP-337 TLS-factory configuration key (removed in Pulsar 5.0, PIP-478) left
     * in a config file. A key that is absent or blank is tolerated (the default); a {@code *Plugin} key still
     * naming the old default factory FQCN ({@link #DEFAULT_PIP337_SSL_FACTORY_CLASS}) is also tolerated (it is
     * equivalent to unset); any other non-blank value fails the load with an actionable migration message
     * pointing to the {@code tlsFactoryClassName} successor.
     *
     * @param properties the loaded configuration properties
     * @throws IllegalArgumentException if a removed PIP-337 key is present with a non-default value
     */
    static void rejectRemovedPip337TlsFactoryKeys(Properties properties) {
        for (String key : REMOVED_PIP337_TLS_FACTORY_KEYS) {
            String value = properties.getProperty(key);
            if (StringUtils.isBlank(value)) {
                continue;
            }
            // A *Plugin key still set to the old default factory FQCN means no custom factory — tolerate it as
            // if unset. The *PluginParams keys carry no default value, so any non-blank value is rejected.
            if (key.endsWith("Plugin") && DEFAULT_PIP337_SSL_FACTORY_CLASS.equals(value.trim())) {
                continue;
            }
            throw new IllegalArgumentException("The PIP-337 '" + key + "' configuration key is removed in "
                    + "Pulsar 5.0 (PIP-478). Migrate the custom SSL factory to a PulsarTlsFactory selected by "
                    + "tlsFactoryClassName / tlsFactoryConfig (or brokerClientTlsFactoryClassName / "
                    + "brokerClientTlsFactoryConfig) and remove '" + key + "' from the configuration.");
        }
    }

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values loaded from provided property file.
     *
     * @param configFile
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static <T extends PulsarConfiguration> T create(String configFile,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        requireNonNull(configFile);
        try (InputStream inputStream = new FileInputStream(configFile)) {
            return create(inputStream, clazz);
        }
    }

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values loaded from provided inputstream
     * property file.
     *
     * @param inStream
     * @throws IOException
     *             if an error occurred when reading from the input stream.
     * @throws IllegalArgumentException
     *             if the input stream contains incorrect value type
     */
    public static <T extends PulsarConfiguration> T create(InputStream inStream,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        try {
            requireNonNull(inStream);
            Properties properties = new Properties();
            properties.load(inStream);
            return (create(properties, clazz));
        } finally {
            if (inStream != null) {
                inStream.close();
            }
        }
    }

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values from provided Properties object.
     *
     * @param properties The properties to populate the attributed from
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T extends PulsarConfiguration> T create(Properties properties,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        requireNonNull(properties);
        // PIP-478: reject a stale, removed PIP-337 sslFactoryPlugin key rather than silently ignoring it.
        rejectRemovedPip337TlsFactoryKeys(properties);
        T configuration;
        try {
            configuration = (T) clazz.getDeclaredConstructor().newInstance();
            configuration.setProperties(properties);
            update((Map) properties, configuration);
        } catch (InstantiationException | IllegalAccessException
                | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalArgumentException("Failed to instantiate " + clazz.getName(), e);
        }
        return configuration;
    }

    /**
     * Validates {@link FieldContext} annotation on each field of the class element. If element is annotated required
     * and value of the element is null or number value is not in a provided (min,max) range then consider as incomplete
     * object and throws exception with incomplete parameters
     *
     * @param obj
     * @return
     * @throws IllegalArgumentException
     *             if object is field values are not completed according to {@link FieldContext} constraints.
     * @throws IllegalAccessException
     */
    public static boolean isComplete(Object obj) throws IllegalArgumentException {
        requireNonNull(obj);
        Field[] fields = obj.getClass().getDeclaredFields();
        StringBuilder error = new StringBuilder();
        for (Field field : fields) {
            if (field.isAnnotationPresent(FieldContext.class)) {
                field.setAccessible(true);
                Object value;

                try {
                    value = field.get(obj);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                log.debug()
                        .attr("field", field.getName())
                        .attr("value", value)
                        .log("Validating configuration field");
                boolean isRequired = field.getAnnotation(FieldContext.class).required();
                long minValue = field.getAnnotation(FieldContext.class).minValue();
                long maxValue = field.getAnnotation(FieldContext.class).maxValue();
                if (isRequired && isEmpty(value)) {
                    error.append(String.format("Required %s is null,", field.getName()));
                }

                if (value != null && Number.class.isAssignableFrom(value.getClass())) {
                    long fieldVal = ((Number) value).longValue();
                    boolean valid = fieldVal >= minValue && fieldVal <= maxValue;
                    if (!valid) {
                        error.append(String.format("%s value %d doesn't fit in given range (%d, %d),", field.getName(),
                                fieldVal, minValue, maxValue));
                    }
                }
            }
        }
        if (error.length() > 0) {
            throw new IllegalArgumentException(error.substring(0, error.length() - 1));
        }
        return true;
    }

    private static boolean isEmpty(Object obj) {
        if (obj == null) {
            return true;
        } else if (obj instanceof String) {
            return StringUtils.isBlank((String) obj);
        } else {
            return false;
        }
    }

    /**
     * Converts a PulsarConfiguration object to a ServiceConfiguration object.
     *
     * @param conf
     * @param ignoreNonExistMember
     * @return
     * @throws IllegalArgumentException
     *             if conf has the field whose name is not contained in ServiceConfiguration and ignoreNonExistMember
     *             is false.
     * @throws RuntimeException
     */
    public static ServiceConfiguration convertFrom(PulsarConfiguration conf, boolean ignoreNonExistMember)
            throws RuntimeException {
        try {
            final ServiceConfiguration convertedConf = ServiceConfiguration.class
                    .getDeclaredConstructor().newInstance();
            Field[] confFields = conf.getClass().getDeclaredFields();
            Properties sourceProperties = conf.getProperties();
            Properties targetProperties = convertedConf.getProperties();
            Arrays.stream(confFields).forEach(confField -> {
                try {
                    confField.setAccessible(true);
                    Field convertedConfField = ServiceConfiguration.class.getDeclaredField(confField.getName());
                    if (!Modifier.isStatic(convertedConfField.getModifiers())
                            && convertedConfField.getDeclaredAnnotation(FieldContext.class) != null) {
                        convertedConfField.setAccessible(true);
                        convertedConfField.set(convertedConf, confField.get(conf));
                    }
                } catch (NoSuchFieldException e) {
                    if (!ignoreNonExistMember) {
                        throw new IllegalArgumentException(
                                "Exception caused while converting configuration: " + e.getMessage());
                    }
                    // add unknown fields to properties
                    try {
                        String propertyName = confField.getName();
                        if (!sourceProperties.containsKey(propertyName) && confField.get(conf) != null) {
                            targetProperties.put(propertyName, confField.get(conf));
                        }
                    } catch (Exception ignoreException) {
                        // should not happen
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Exception caused while converting configuration: " + e.getMessage());
                }
            });
            // Put the rest of properties to new config
            targetProperties.putAll(sourceProperties);
            return convertedConf;
        } catch (InstantiationException | IllegalAccessException
                | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Exception caused while converting configuration: " + e.getMessage());
        }
    }

    public static ServiceConfiguration convertFrom(PulsarConfiguration conf) throws RuntimeException {
        return convertFrom(conf, true);
    }

    /**
     * Returns the subset of configuration whose values differ from the defaults of a freshly-instantiated
     * configuration of the same class. Any entries in {@link PulsarConfiguration#getProperties()} that are not
     * declared fields are also included. Useful to log only the user-provided overrides instead of the full
     * configuration.
     */
    public static Map<String, Object> runtimeConfigurationOverrides(PulsarConfiguration conf) {
        try {
            PulsarConfiguration defaults = conf.getClass().getDeclaredConstructor().newInstance();
            Map<String, Object> overrides = new TreeMap<>();
            for (Field field : conf.getClass().getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                if (field.getDeclaredAnnotation(FieldContext.class) == null) {
                    continue;
                }
                field.setAccessible(true);
                Object current = field.get(conf);
                Object def = field.get(defaults);
                if (!Objects.equals(current, def)) {
                    overrides.put(field.getName(), current);
                }
            }
            Properties props = conf.getProperties();
            if (props != null) {
                for (String key : props.stringPropertyNames()) {
                    overrides.putIfAbsent(key, props.getProperty(key));
                }
            }
            return overrides;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to compute configuration overrides", e);
        }
    }
}
