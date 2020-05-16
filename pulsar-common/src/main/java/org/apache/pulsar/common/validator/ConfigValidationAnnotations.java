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
package org.apache.pulsar.common.validator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This class defines all the annotations that classes can use to do their field validations.
 */
public class ConfigValidationAnnotations {

    // CHECKSTYLE.OFF: TypeNameCheck

    /**
     * Validates on object is not null.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface NotNull {
        Class<?> validatorClass() default ValidatorImpls.NotNullValidator.class;
    }

    /**
     * Checks if a number is positive and whether zero inclusive Validator with fields: validatorClass, includeZero.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isPositiveNumber {
        Class<?> validatorClass() default ValidatorImpls.PositiveNumberValidator.class;

        boolean includeZero() default false;
    }

    /**
     * Checks if the field satisfies the custom validator class.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isCustomFormat {
        Class<?> validatorClass();
    }

    /**
     * validates each entry in a list is of a certain type.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isListEntryType {
        Class<?> validatorClass() default ValidatorImpls.ListEntryTypeValidator.class;

        Class<?> type();
    }

    /**
     * validates each entry in a list is of String type.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isStringList {
        Class<?> validatorClass() default ValidatorImpls.ListEntryTypeValidator.class;

        Class<?> type() default String.class;
    }

    /**
     * Validates each entry in a list with a list of validators Validators with
     * fields: validatorClass and entryValidatorClass.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isListEntryCustom {
        Class<?> validatorClass() default ValidatorImpls.ListEntryCustomValidator.class;

        Class<?>[] entryValidatorClasses();
    }


    /**
     * Validates the type of each key and value in a map Validator with
     * fields: validatorClass, keyValidatorClass, valueValidatorClass.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isMapEntryType {
        Class<?> validatorClass() default ValidatorImpls.MapEntryTypeValidator.class;

        Class<?> keyType();

        Class<?> valueType();
    }

    /**
     * Checks if class name is assignable to the provided class/interfaces.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isImplementationOfClass {
        Class<?> validatorClass() default ValidatorImpls.ImplementsClassValidator.class;

        Class<?> implementsClass();
    }

    /**
     * Checks if class name is assignable to ONE of the provided list class/interfaces.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isImplementationOfClasses {
        Class<?> validatorClass() default ValidatorImpls.ImplementsClassesValidator.class;

        Class<?>[] implementsClasses();
    }

    /**
     * Validates a each key and value in a Map with a list of validators Validator with
     * fields: validatorClass, keyValidatorClasses, valueValidatorClasses.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isMapEntryCustom {
        Class<?> validatorClass() default ValidatorImpls.MapEntryCustomValidator.class;

        Class<?>[] keyValidatorClasses();

        Class<?>[] valueValidatorClasses();
    }

    /**
     * checks if the topic name is valid.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isValidTopicName {
        Class<?> validatorClass() default ValidatorImpls.TopicNameValidator.class;
    }

    /**
     * Field names for annotations.
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
    }

    // CHECKSTYLE.ON: TypeNameCheck
}