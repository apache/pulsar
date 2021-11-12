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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

/**
 * System defined Validator Annotations.
 */
public class ValidatorImpls {

    private static final Logger log = LoggerFactory.getLogger(ValidatorImpls.class);

    /**
     * Validates a positive number.
     */
    public static class PositiveNumberValidator extends Validator {

        private boolean includeZero;

        public PositiveNumberValidator() {
            this.includeZero = false;
        }

        public PositiveNumberValidator(Map<String, Object> params) {
            this.includeZero = (boolean) params.get(ConfigValidationAnnotations.ValidatorParams.INCLUDE_ZERO);
        }

        public static void validateField(String name, boolean includeZero, Object o) {
            if (o == null) {
                return;
            }
            if (o instanceof Number) {
                if (includeZero) {
                    if (((Number) o).doubleValue() >= 0.0) {
                        return;
                    }
                } else {
                    if (((Number) o).doubleValue() > 0.0) {
                        return;
                    }
                }
            }
            throw new IllegalArgumentException(String.format("Field '%s' must be a Positive Number", name));
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.includeZero, o);
        }
    }

    /**
     * Validates if an object is not null.
     */

    public static class NotNullValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                throw new IllegalArgumentException(String.format("Field '%s' cannot be null!", name));
            }
        }
    }

    /**
     * Validates each entry in a list.
     */
    public static class ListEntryTypeValidator extends Validator {

        private Class<?> type;

        public ListEntryTypeValidator(Map<String, Object> params) {
            this.type = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.TYPE);
        }

        public static void validateField(String name, Class<?> type, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.listFv(type, false);
            validator.validateField(name, o);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.type, o);
        }
    }

    /**
     * validates each key and value in a map of a certain type.
     */
    public static class MapEntryTypeValidator extends Validator {

        private Class<?> keyType;
        private Class<?> valueType;

        public MapEntryTypeValidator(Map<String, Object> params) {
            this.keyType = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.KEY_TYPE);
            this.valueType = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.VALUE_TYPE);
        }

        public static void validateField(String name, Class<?> keyType, Class<?> valueType, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(keyType,
                    valueType, false);
            validator.validateField(name, o);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.keyType, this.valueType, o);
        }
    }

    /**
     * validates that the item implements a certain type.
     */
    public static class ImplementsClassValidator extends Validator {

        Class<?> classImplements;

        public ImplementsClassValidator(Map<String, Object> params) {
            this.classImplements = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.IMPLEMENTS_CLASS);
        }

        public ImplementsClassValidator(Class<?> classImplements) {
            this.classImplements = classImplements;
        }

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, String.class, o);
            String className = (String) o;
            try {
                ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
                Class<?> objectClass = clsLoader.loadClass(className);
                if (!this.classImplements.isAssignableFrom(objectClass)) {
                    throw new IllegalArgumentException(
                            String.format("Field '%s' with value '%s' does not implement %s ",
                                    name, o, this.classImplements.getName()));
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * validates class implements one of these classes.
     */
    public static class ImplementsClassesValidator extends Validator {

        Class<?>[] classesImplements;

        public ImplementsClassesValidator(Map<String, Object> params) {
            this.classesImplements = (Class<?>[]) params.get(
                    ConfigValidationAnnotations.ValidatorParams.IMPLEMENTS_CLASSES);
        }

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, String.class, o);
            String className = (String) o;
            int count = 0;
            for (Class<?> classImplements : classesImplements) {
                Class<?> objectClass = null;
                try {
                    objectClass = loadClass(className);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Cannot find/load class " + className);
                }

                if (classImplements.isAssignableFrom(objectClass)) {
                    count++;
                }
            }
            if (count == 0) {
                throw new IllegalArgumentException(
                        String.format("Field '%s' with value '%s' does not implement any of these classes %s",
                                name, o, Arrays.toString(classesImplements)));
            }
        }
    }

    /**
     * validates each key and each value against the respective arrays of validators.
     */
    public static class MapEntryCustomValidator extends Validator {

        private Class<?>[] keyValidators;
        private Class<?>[] valueValidators;

        public MapEntryCustomValidator(Map<String, Object> params) {
            this.keyValidators = (Class<?>[]) params.get(
                    ConfigValidationAnnotations.ValidatorParams.KEY_VALIDATOR_CLASSES);
            this.valueValidators = (Class<?>[]) params.get(
                    ConfigValidationAnnotations.ValidatorParams.VALUE_VALIDATOR_CLASSES);
        }

        @SuppressWarnings("unchecked")
        public static void validateField(String name, Class<?>[] keyValidators, Class<?>[] valueValidators, Object o)
                throws IllegalAccessException, InstantiationException,
                NoSuchMethodException, InvocationTargetException {
            if (o == null) {
                return;
            }
            //check if Map
            SimpleTypeValidator.validateField(name, Map.class, o);
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) o).entrySet()) {
                for (Class<?> kv : keyValidators) {
                    Object keyValidator = kv.getConstructor().newInstance();
                    if (keyValidator instanceof Validator) {
                        ((Validator) keyValidator).validateField(name + " Map key", entry.getKey());
                    } else {
                        log.warn(
                                "validator: {} cannot be used in MapEntryCustomValidator to validate keys. "
                                + " Individual entry validators must be a instance of Validator class",
                                kv.getName());
                    }
                }
                for (Class<?> vv : valueValidators) {
                    Object valueValidator = vv.getConstructor().newInstance();
                    if (valueValidator instanceof Validator) {
                        ((Validator) valueValidator).validateField(name + " Map value", entry.getValue());
                    } else {
                        log.warn(
                                "validator: {} cannot be used in MapEntryCustomValidator to validate values. "
                                + " Individual entry validators must a instance of Validator class",
                                vv.getName());
                    }
                }
            }
        }

        @Override
        public void validateField(String name, Object o) {
            try {
                validateField(name, this.keyValidators, this.valueValidators, o);
            } catch (IllegalAccessException | InstantiationException
                    | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * validates that the string is equal to one of the specified ones in the list.
     */
    public static class StringValidator extends Validator {

        public StringValidator() {

        }

        private HashSet<String> acceptedValues = null;

        public StringValidator(Map<String, Object> params) {

            this.acceptedValues =
                    new HashSet<String>(Arrays.asList((String[]) params.get(
                            ConfigValidationAnnotations.ValidatorParams.ACCEPTED_VALUES)));

            if (this.acceptedValues.isEmpty()
                    || (this.acceptedValues.size() == 1 && this.acceptedValues.contains(""))) {
                this.acceptedValues = null;
            }
        }

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, String.class, o);
            if (this.acceptedValues != null) {
                if (!this.acceptedValues.contains((String) o)) {
                    throw new IllegalArgumentException(
                            "Field " + name + " is not an accepted value. Value: " + o
                                    + " Accepted values: " + this.acceptedValues);
                }
            }
        }
    }

    /**
     * Validates each entry in a list against a list of custom Validators. Each validator in the
     * list of validators must inherit or be an instance of Validator class
     */
    public static class ListEntryCustomValidator extends Validator {

        private Class<?>[] entryValidators;

        public ListEntryCustomValidator(Map<String, Object> params) {
            this.entryValidators = (Class<?>[]) params.get(
                    ConfigValidationAnnotations.ValidatorParams.ENTRY_VALIDATOR_CLASSES);
        }

        public static void validateField(String name, Class<?>[] validators, Object o)
                throws IllegalAccessException, InstantiationException,
                NoSuchMethodException, InvocationTargetException {
            if (o == null) {
                return;
            }
            //check if iterable
            SimpleTypeValidator.validateField(name, Iterable.class, o);
            for (Object entry : (Iterable<?>) o) {
                for (Class<?> validator : validators) {
                    Object v = validator.getConstructor().newInstance();
                    if (v instanceof Validator) {
                        ((Validator) v).validateField(name + " list entry", entry);
                    } else {
                        log.warn(
                                "validator: {} cannot be used in ListEntryCustomValidator."
                                + " Individual entry validators must a instance of Validator class",
                                validator.getName());
                    }
                }
            }
        }

        @Override
        public void validateField(String name, Object o) {
            try {
                validateField(name, this.entryValidators, o);
            } catch (NoSuchMethodException | IllegalAccessException
                    | InvocationTargetException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Validates basic types.
     */
    public static class SimpleTypeValidator extends Validator {

        private Class<?> type;

        public SimpleTypeValidator(Map<String, Object> params) {
            this.type = (Class<?>) params.get(ConfigValidationAnnotations.ValidatorParams.TYPE);
        }

        public static void validateField(String name, Class<?> type, Object o) {
            if (o == null) {
                return;
            }
            if (type.isInstance(o)) {
                return;
            }
            throw new IllegalArgumentException(
                    "Field " + name + " must be of type " + type + ". Object: " + o + " actual type: " + o.getClass());
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.type, o);
        }
    }

    private static Class<?> loadClass(String className) throws ClassNotFoundException {
        Class<?> objectClass;
        try {
            objectClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            if (clsLoader != null) {
                objectClass = clsLoader.loadClass(className);
            } else {
                throw e;
            }
        }
        return objectClass;
    }
}