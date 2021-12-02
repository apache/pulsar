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
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads ServiceConfiguration with properties
 *
 *
 */
public class PulsarConfigurationLoader {

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
        T configuration = null;
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

                if (log.isDebugEnabled()) {
                    log.debug("Validating configuration field '{}' = '{}'", field.getName(), value);
                }
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
     *             if conf has the field whose name is not contained in ServiceConfiguration and ignoreNonExistMember is false.
     * @throws RuntimeException
     */
    public static ServiceConfiguration convertFrom(PulsarConfiguration conf, boolean ignoreNonExistMember) throws RuntimeException {
        try {
            final ServiceConfiguration convertedConf = ServiceConfiguration.class
                    .getDeclaredConstructor().newInstance();
            Field[] confFields = conf.getClass().getDeclaredFields();
            Arrays.stream(confFields).forEach(confField -> {
                try {
                    Field convertedConfField = ServiceConfiguration.class.getDeclaredField(confField.getName());
                    confField.setAccessible(true);
                    if (!Modifier.isStatic(convertedConfField.getModifiers())) {
                        convertedConfField.setAccessible(true);
                        convertedConfField.set(convertedConf, confField.get(conf));
                    }
                } catch (NoSuchFieldException e) {
                    if (!ignoreNonExistMember) {
                        throw new IllegalArgumentException("Exception caused while converting configuration: " + e.getMessage());
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Exception caused while converting configuration: " + e.getMessage());
                }
            });
            return convertedConf;
        } catch (InstantiationException | IllegalAccessException
                | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Exception caused while converting configuration: " + e.getMessage());
        }
    }

    public static ServiceConfiguration convertFrom(PulsarConfiguration conf) throws RuntimeException {
        return convertFrom(conf, true);
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarConfigurationLoader.class);
}
