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

import java.lang.reflect.Field;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.FieldParser;

/**
 * Validates {@link FieldContext} value constraints ({@code required}, numeric bounds,
 * {@code maxCharLength}, and boolean format for string inputs) on reflected configuration fields.
 * <p>
 * Used when validating configuration completeness at startup
 * ({@link PulsarConfigurationLoader#isComplete}) and when validating dynamic configuration
 * updates via the admin API.
 */
public final class FieldContextValidator {

    private FieldContextValidator() {
    }

    /**
     * Validates a string configuration value for a reflected field.
     * <p>
     * Parses {@code valueStr} with {@link FieldParser}, then checks {@code required},
     * numeric bounds, {@code maxCharLength}, and boolean format. Blank {@code valueStr}
     * is valid for optional fields; for dynamic configuration, blank means clearing the
     * override.
     *
     * @param field    configuration field
     * @param valueStr raw string value from the admin API
     * @return error message if validation fails, or empty if the field has no
     *         {@link FieldContext} or the value passes
     */
    public static Optional<String> validateString(Field field, String valueStr) {
        if (field == null || !field.isAnnotationPresent(FieldContext.class)) {
            return Optional.empty();
        }
        FieldContext fieldContext = field.getAnnotation(FieldContext.class);
        if (StringUtils.isBlank(valueStr)) {
            if (fieldContext.required()) {
                return Optional.of(String.format("Required %s is null", field.getName()));
            }
            return Optional.empty();
        }
        if (isBooleanField(field) && !isValidBooleanString(valueStr)) {
            return Optional.of(String.format("%s must be 'true' or 'false'", field.getName()));
        }
        final Object parsedValue;
        try {
            parsedValue = FieldParser.value(valueStr, field);
        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return Optional.of(String.format("Failed to parse %s: %s", field.getName(), message));
        }
        return validateParsedValue(field, parsedValue);
    }

    /**
     * Validates a configuration value that is already parsed to the field's Java type.
     * <p>
     * Checks {@code required}, numeric {@code minValue}/{@code maxValue}, and
     * {@code maxCharLength} from {@link FieldContext}. Does not parse strings or validate
     * boolean string format.
     *
     * @param field configuration field
     * @param value value in the field's Java type
     * @return error message if validation fails, or empty if the field has no
     *         {@link FieldContext} or the value passes
     */
    public static Optional<String> validateParsedValue(Field field, Object value) {
        if (field == null || !field.isAnnotationPresent(FieldContext.class)) {
            return Optional.empty();
        }
        FieldContext fieldContext = field.getAnnotation(FieldContext.class);
        if (fieldContext.required() && isEmpty(value)) {
            return Optional.of(String.format("Required %s is null", field.getName()));
        }
        if (value != null && Number.class.isAssignableFrom(value.getClass())) {
            long fieldVal = ((Number) value).longValue();
            long minValue = fieldContext.minValue();
            long maxValue = fieldContext.maxValue();
            if (fieldVal < minValue || fieldVal > maxValue) {
                return Optional.of(String.format("%s value %d doesn't fit in given range (%d, %d)",
                        field.getName(), fieldVal, minValue, maxValue));
            }
        }
        if (value instanceof String stringValue && stringValue.length() > fieldContext.maxCharLength()) {
            return Optional.of(String.format("%s exceeds maxCharLength %d",
                    field.getName(), fieldContext.maxCharLength()));
        }
        return Optional.empty();
    }

    private static boolean isBooleanField(Field field) {
        Class<?> type = field.getType();
        return type == boolean.class || type == Boolean.class;
    }

    private static boolean isValidBooleanString(String valueStr) {
        return "true".equalsIgnoreCase(valueStr) || "false".equalsIgnoreCase(valueStr);
    }

    private static boolean isEmpty(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof String stringValue) {
            return StringUtils.isBlank(stringValue);
        }
        return false;
    }
}
