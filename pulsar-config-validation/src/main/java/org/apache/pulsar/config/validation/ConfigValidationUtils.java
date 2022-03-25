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
package org.apache.pulsar.config.validation;

import java.util.Map;

/**
 * Helper methods for ConfigValidationAnnotations.
 */
public class ConfigValidationUtils {
    /**
     * Returns a new NestableFieldValidator for a given class.
     *
     * @param cls     the Class the field should be a type of
     * @param notNull whether or not a value of null is valid
     * @return a NestableFieldValidator for that class
     */
    public static NestableFieldValidator fv(final Class cls, final boolean notNull) {
        return new NestableFieldValidator() {
            @Override
            public void validateField(String pd, String name, Object field)
                    throws IllegalArgumentException {
                if (field == null) {
                    if (notNull) {
                        throw new IllegalArgumentException("Field " + name + " must not be null");
                    } else {
                        return;
                    }
                }
                if (!cls.isInstance(field)) {
                    throw new IllegalArgumentException(
                            pd + name + " must be a " + cls.getName() + ". (" + field + ")");
                }
            }
        };
    }

    /**
     * Returns a new NestableFieldValidator for a List of the given Class.
     *
     * @param cls     the Class of elements composing the list
     * @param notNull whether or not a value of null is valid
     * @return a NestableFieldValidator for a list of the given class
     */
    public static NestableFieldValidator listFv(Class cls, boolean notNull) {
        return listFv(fv(cls, notNull), notNull);
    }

    /**
     * Returns a new NestableFieldValidator for a List where each item is validated by validator.
     *
     * @param validator used to validate each item in the list
     * @param notNull   whether or not a value of null is valid
     * @return a NestableFieldValidator for a list with each item validated by a different validator.
     */
    public static NestableFieldValidator listFv(final NestableFieldValidator validator,
                                                final boolean notNull) {
        return new NestableFieldValidator() {
            @Override
            public void validateField(String pd, String name, Object field)
                    throws IllegalArgumentException {

                if (field == null) {
                    if (notNull) {
                        throw new IllegalArgumentException("Field " + name + " must not be null");
                    } else {
                        return;
                    }
                }
                if (field instanceof Iterable) {
                    for (Object e : (Iterable) field) {
                        validator.validateField(pd + "Each element of the list ", name, e);
                    }
                    return;
                }
                throw new IllegalArgumentException(
                        "Field " + name + " must be an Iterable but was a " + field.getClass());
            }
        };
    }

    /**
     * Returns a new NestableFieldValidator for a Map of key to val.
     *
     * @param key     the Class of keys in the map
     * @param val     the Class of values in the map
     * @param notNull whether or not a value of null is valid
     * @return a NestableFieldValidator for a Map of key to val
     */
    public static NestableFieldValidator mapFv(Class key, Class val,
                                               boolean notNull) {
        return mapFv(fv(key, false), fv(val, false), notNull);
    }

    /**
     * Returns a new NestableFieldValidator for a Map.
     *
     * @param key     a validator for the keys in the map
     * @param val     a validator for the values in the map
     * @param notNull whether or not a value of null is valid
     * @return a NestableFieldValidator for a Map
     */
    public static NestableFieldValidator mapFv(final NestableFieldValidator key,
                                               final NestableFieldValidator val, final boolean notNull) {
        return new NestableFieldValidator() {
            @SuppressWarnings("unchecked")
            @Override
            public void validateField(String pd, String name, Object field)
                    throws IllegalArgumentException {
                if (field == null) {
                    if (notNull) {
                        throw new IllegalArgumentException("Field " + name + " must not be null");
                    } else {
                        return;
                    }
                }
                if (field instanceof Map) {
                    for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) field).entrySet()) {
                        key.validateField("Each key of the map ", name, entry.getKey());
                        val.validateField("Each value in the map ", name, entry.getValue());
                    }
                    return;
                }
                throw new IllegalArgumentException(
                        "Field " + name + " must be a Map");
            }
        };
    }

    /**
     * Declares methods for validating configuration values.
     */
    public interface FieldValidator {
        /**
         * Validates the given field.
         *
         * @param name  the name of the field.
         * @param field The field to be validated.
         * @throws IllegalArgumentException if the field fails validation.
         */
        void validateField(String name, Object field) throws IllegalArgumentException;
    }

    /**
     * Declares a method for validating configuration values that is nestable.
     */
    public abstract static class NestableFieldValidator implements FieldValidator {
        @Override
        public void validateField(String name, Object field) throws IllegalArgumentException {
            validateField(null, name, field);
        }

        /**
         * Validates the given field.
         *
         * @param pd    describes the parent wrapping this validator.
         * @param name  the name of the field.
         * @param field The field to be validated.
         * @throws IllegalArgumentException if the field fails validation.
         */
        public abstract void validateField(String pd, String name, Object field) throws IllegalArgumentException;
    }
}