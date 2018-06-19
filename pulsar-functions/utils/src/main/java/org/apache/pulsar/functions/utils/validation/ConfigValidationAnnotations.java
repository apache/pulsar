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
package org.apache.pulsar.functions.utils.validation;

import org.apache.pulsar.functions.utils.FunctionConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class ConfigValidationAnnotations {

    /**
     * Validates on object is not null
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface NotNull {
        Class<?> validatorClass() default ValidatorImpls.NotNullValidator.class;

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    /**
     * Checks if a number is positive and whether zero inclusive Validator with fields: validatorClass, includeZero
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isPositiveNumber {
        Class<?> validatorClass() default ValidatorImpls.PositiveNumberValidator.class;

        boolean includeZero() default false;

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }


    /**
     * Checks if resources specified are valid
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isValidResources {

        Class<?> validatorClass() default ValidatorImpls.ResourcesValidator.class;

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    /**
     * validates each entry in a list is of a certain type
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isListEntryType {
        Class<?> validatorClass() default ValidatorImpls.ListEntryTypeValidator.class;

        Class<?> type();

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isStringList {
        Class<?> validatorClass() default ValidatorImpls.ListEntryTypeValidator.class;

        Class<?> type() default String.class;

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    /**
     * Validates each entry in a list with a list of validators Validators with fields: validatorClass and entryValidatorClass
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isListEntryCustom {
        Class<?> validatorClass() default ValidatorImpls.ListEntryCustomValidator.class;

        Class<?>[] entryValidatorClasses();

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }


    /**
     * Validates the type of each key and value in a map Validator with fields: validatorClass, keyValidatorClass, valueValidatorClass
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isMapEntryType {
        Class<?> validatorClass() default ValidatorImpls.MapEntryTypeValidator.class;

        Class<?> keyType();

        Class<?> valueType();

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    /**
     * Checks if class name is assignable to the provided class/interfaces
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isImplementationOfClass {
        Class<?> validatorClass() default ValidatorImpls.ImplementsClassValidator.class;

        Class<?> implementsClass();

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.JAVA;
    }

    /**
     * Checks if class name is assignable to ONE of the provided list class/interfaces
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isImplementationOfClasses {
        Class<?> validatorClass() default ValidatorImpls.ImplementsClassesValidator.class;

        Class<?>[] implementsClasses();

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.JAVA;
    }

    /**
     * Validates a each key and value in a Map with a list of validators Validator with fields: validatorClass, keyValidatorClasses,
     * valueValidatorClasses
     */
    @Repeatable(isMapEntryCustoms.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isMapEntryCustom {
        Class<?> validatorClass() default ValidatorImpls.MapEntryCustomValidator.class;

        Class<?>[] keyValidatorClasses() default {};

        Class<?>[] valueValidatorClasses() default {};

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isMapEntryCustoms {
        isMapEntryCustom[] value();
    }

    /**
     * checks if the topic name is valid
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isValidTopicName {
        Class<?> validatorClass() default ValidatorImpls.TopicNameValidator.class;

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    /**
     * checks if window configs is valid
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isValidWindowConfig {
        Class<?> validatorClass() default ValidatorImpls.WindowConfigValidator.class;

        ConfigValidation.Runtime targetRuntime() default ConfigValidation.Runtime.ALL;
    }

    /**
     * check if file exists
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isFileExists {
        Class<?> validatorClass() default ValidatorImpls.FileValidator.class;
    }

    /**
     * checks function config as a whole to make sure all fields are valid
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface isValidFunctionConfig {
        Class<?> validatorClass() default ValidatorImpls.FunctionConfigValidator.class;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface isValidSourceConfig {
        Class<?> validatorClass() default ValidatorImpls.SourceConfigValidator.class;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE})
    public @interface isValidSinkConfig {
        Class<?> validatorClass() default ValidatorImpls.SinkConfigValidator.class;
    }
    /**
     * Field names for annotations
     */
    public static class ValidatorParams {
        static final String VALIDATOR_CLASS = "validatorClass";
        static final String TYPE = "type";
        static final String BASE_TYPE = "baseType";
        static final String ENTRY_VALIDATOR_CLASSES = "entryValidatorClasses";
        static final String KEY_VALIDATOR_CLASSES = "keyValidatorClasses";
        static final String VALUE_VALIDATOR_CLASSES = "valueValidatorClasses";
        static final String KEY_TYPE = "keyType";
        static final String VALUE_TYPE = "valueType";
        static final String INCLUDE_ZERO = "includeZero";
        static final String ACCEPTED_VALUES = "acceptedValues";
        static final String IMPLEMENTS_CLASS = "implementsClass";
        static final String IMPLEMENTS_CLASSES = "implementsClasses";
        static final String ACTUAL_RUNTIME = "actualRuntime";
        static final String TARGET_RUNTIME = "targetRuntime";
    }
}
