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

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Resources;
import org.apache.pulsar.functions.utils.SinkConfig;
import org.apache.pulsar.functions.utils.SourceConfig;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.utils.WindowConfig;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.pulsar.functions.utils.Utils.fileExists;
import static org.apache.pulsar.functions.utils.Utils.getSinkType;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;

@Slf4j
public class ValidatorImpls {
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

    @NoArgsConstructor
    public static class ResourcesValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                throw new IllegalArgumentException(String.format("Field '%s' cannot be null!", name));
            }

            if (o instanceof Resources) {
                Resources resources = (Resources) o;
                Double cpu = resources.getCpu();
                Long ram = resources.getRam();
                Long disk = resources.getDisk();
                com.google.common.base.Preconditions.checkArgument(cpu == null || cpu > 0.0,
                        "The cpu allocation for the function must be positive");
                com.google.common.base.Preconditions.checkArgument(ram == null || ram > 0L,
                        "The ram allocation for the function must be positive");
                com.google.common.base.Preconditions.checkArgument(disk == null || disk > 0L,
                        "The disk allocation for the function must be positive");
            } else {
                throw new IllegalArgumentException(String.format("Field '%s' must be of Resource type!", name));
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
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(keyType, valueType, false);
            validator.validateField(name, o);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.keyType, this.valueType, o);
        }
    }

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
            if (StringUtils.isEmpty(className)) {
                return;
            }

            Class<?> objectClass;
            try {
                objectClass = loadClass(className);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Cannot find/load class " + className);
            }

            if (!this.classImplements.isAssignableFrom(objectClass)) {
                throw new IllegalArgumentException(
                        String.format("Field '%s' with value '%s' does not implement %s",
                                name, o, this.classImplements.getName()));
            }
        }
    }

    /**
     * validates class implements one of these classes
     */
    public static class ImplementsClassesValidator extends Validator {

        Class<?>[] classesImplements;

        public ImplementsClassesValidator(Map<String, Object> params) {
            this.classesImplements = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.IMPLEMENTS_CLASSES);
        }

        public ImplementsClassesValidator(Class<?>... classesImplements) {
            this.classesImplements = classesImplements;
        }

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, String.class, o);
            String className = (String) o;
            if (StringUtils.isEmpty(className)) {
                return;
            }
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
                                name, o, Arrays.asList(classesImplements)));
            }
        }
    }

    @NoArgsConstructor
    public static class SerdeValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            new ValidatorImpls.ImplementsClassValidator(SerDe.class).validateField(name, o);
        }
    }

    /**
     * validates each key and each value against the respective arrays of validators.
     */
    public static class MapEntryCustomValidator extends Validator {

        private Class<?>[] keyValidators;
        private Class<?>[] valueValidators;

        public MapEntryCustomValidator(Map<String, Object> params) {
            this.keyValidators = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.KEY_VALIDATOR_CLASSES);
            this.valueValidators = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.VALUE_VALIDATOR_CLASSES);
        }

        @SuppressWarnings("unchecked")
        public static void validateField(String name, Class<?>[] keyValidators, Class<?>[] valueValidators, Object o)
                throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
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
                                "validator: {} cannot be used in MapEntryCustomValidator to validate keys.  Individual entry validators must " +
                                        "a instance of Validator class",
                                kv.getName());
                    }
                }
                for (Class<?> vv : valueValidators) {
                    Object valueValidator = vv.getConstructor().newInstance();
                    if (valueValidator instanceof Validator) {
                        ((Validator) valueValidator).validateField(name + " Map value", entry.getValue());
                    } else {
                        log.warn(
                                "validator: {} cannot be used in MapEntryCustomValidator to validate values.  Individual entry validators " +
                                        "must a instance of Validator class",
                                vv.getName());
                    }
                }
            }
        }

        @Override
        public void validateField(String name, Object o) {
            try {
                validateField(name, this.keyValidators, this.valueValidators, o);
            } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @NoArgsConstructor
    public static class StringValidator extends Validator {

        private HashSet<String> acceptedValues = null;

        public StringValidator(Map<String, Object> params) {

            this.acceptedValues =
                    new HashSet<String>(Arrays.asList((String[]) params.get(ConfigValidationAnnotations.ValidatorParams.ACCEPTED_VALUES)));

            if (this.acceptedValues.isEmpty() || (this.acceptedValues.size() == 1 && this.acceptedValues.contains(""))) {
                this.acceptedValues = null;
            }
        }

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, String.class, o);
            if (this.acceptedValues != null) {
                if (!this.acceptedValues.contains((String) o)) {
                    throw new IllegalArgumentException(
                            "Field " + name + " is not an accepted value. Value: " + o + " Accepted values: " + this.acceptedValues);
                }
            }
        }
    }
    @NoArgsConstructor
    public static class FunctionConfigValidator extends Validator {

        private static void doJavaChecks(FunctionConfig functionConfig, String name) {
            Class<?>[] typeArgs = Utils.getFunctionTypes(functionConfig);

            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();
            // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
            // implements SerDe class
            functionConfig.getCustomSerdeInputs().forEach((topicName, inputSerializer) -> {

                Class<?> serdeClass;
                try {
                    serdeClass = loadClass(inputSerializer);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException(
                            String.format("The input serialization/deserialization class %s does not exist",
                                    inputSerializer));
                }

                try {
                    new ValidatorImpls.ImplementsClassValidator(SerDe.class).validateField(name, inputSerializer);
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException(
                            String.format("The input serialization/deserialization class %s does not not implement %s",

                                    inputSerializer, SerDe.class.getCanonicalName()));
                }

                if (inputSerializer.equals(DefaultSerDe.class.getName())) {
                    if (!DefaultSerDe.IsSupportedType(typeArgs[0])) {
                        throw new IllegalArgumentException("The default Serializer does not support type " +
                                typeArgs[0]);
                    }
                } else {
                    SerDe serDe = (SerDe) Reflections.createInstance(inputSerializer, clsLoader);
                    if (serDe == null) {
                        throw new IllegalArgumentException(String.format("The SerDe class %s does not exist",
                                inputSerializer));
                    }
                    Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

                    // type inheritance information seems to be lost in generic type
                    // load the actual type class for verification
                    Class<?> fnInputClass;
                    Class<?> serdeInputClass;
                    try {
                        fnInputClass = Class.forName(typeArgs[0].getName(), true, clsLoader);
                        serdeInputClass = Class.forName(serDeTypes[0].getName(), true, clsLoader);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Failed to load type class", e);
                    }

                    if (!fnInputClass.isAssignableFrom(serdeInputClass)) {
                        throw new IllegalArgumentException("Serializer type mismatch " + typeArgs[0] + " vs " + serDeTypes[0]);
                    }
                }
            });
            functionConfig.getInputs().forEach((topicName) -> {
                if (!DefaultSerDe.IsSupportedType(typeArgs[0])) {
                    throw new RuntimeException("Default Serializer does not support type " + typeArgs[0]);
                }
            });
            if (!Void.class.equals(typeArgs[1])) {
                if (functionConfig.getOutputSerdeClassName() == null
                        || functionConfig.getOutputSerdeClassName().isEmpty()
                        || functionConfig.getOutputSerdeClassName().equals(DefaultSerDe.class.getName())) {
                    if (!DefaultSerDe.IsSupportedType(typeArgs[1])) {
                        throw new RuntimeException("Default Serializer does not support type " + typeArgs[1]);
                    }
                } else {
                    SerDe serDe = (SerDe) Reflections.createInstance(functionConfig.getOutputSerdeClassName(),
                            clsLoader);
                    if (serDe == null) {
                        throw new IllegalArgumentException(String.format("SerDe class %s does not exist",
                                functionConfig.getOutputSerdeClassName()));
                    }
                    Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

                    // type inheritance information seems to be lost in generic type
                    // load the actual type class for verification
                    Class<?> fnOutputClass;
                    Class<?> serdeOutputClass;
                    try {
                        fnOutputClass = Class.forName(typeArgs[1].getName(), true, clsLoader);
                        serdeOutputClass = Class.forName(serDeTypes[0].getName(), true, clsLoader);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException("Failed to load type class", e);
                    }

                    if (!serdeOutputClass.isAssignableFrom(fnOutputClass)) {
                        throw new RuntimeException("Serializer type mismatch " + typeArgs[1] + " vs " + serDeTypes[0]);
                    }
                }
            }
        }

        private static void doPythonChecks(FunctionConfig functionConfig, String name) {
            if (functionConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                throw new RuntimeException("Effectively-once processing guarantees not yet supported in Python");
            }

            if (functionConfig.getWindowConfig() != null) {
                throw new IllegalArgumentException("There is currently no support windowing in python");
            }

            if (StringUtils.isNotBlank(functionConfig.getTopicsPattern())) {
                throw new IllegalArgumentException("Topic-patterns is not supported for python runtime");
            }
        }

        private static void verifyNoTopicClash(Collection<String> inputTopics, String outputTopic) throws IllegalArgumentException {
            if (inputTopics.contains(outputTopic)) {
                throw new IllegalArgumentException(
                        String.format("Output topic %s is also being used as an input topic (topics must be one or the other)",
                                outputTopic));
            }
        }

        private static void doCommonChecks(FunctionConfig functionConfig) {
            if ((functionConfig.getInputs().isEmpty() && StringUtils.isEmpty(functionConfig.getTopicsPattern()))
                    && functionConfig.getCustomSerdeInputs().isEmpty()) {
                throw new RuntimeException("No input topic(s) specified for the function");
            }

            // Ensure that topics aren't being used as both input and output
            verifyNoTopicClash(functionConfig.getInputs(), functionConfig.getOutput());

            WindowConfig windowConfig = functionConfig.getWindowConfig();
            if (windowConfig != null) {
                // set auto ack to false since windowing framework is responsible
                // for acking and not the function framework
                if (functionConfig.isAutoAck() == true) {
                    throw new IllegalArgumentException("Cannot enable auto ack when using windowing functionality");
                }
                functionConfig.setAutoAck(false);
            }

            if (functionConfig.getTimeoutMs() != null
                    && functionConfig.getProcessingGuarantees() != null
                    && functionConfig.getProcessingGuarantees() != FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                throw new IllegalArgumentException("Message timeout can only be specifed with processing guarantee is "
                        + FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE.name());
            }
        }

        @Override
        public void validateField(String name, Object o) {
            FunctionConfig functionConfig = (FunctionConfig) o;
            doCommonChecks(functionConfig);
            if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
                doJavaChecks(functionConfig, name);
            } else {
                doPythonChecks(functionConfig, name);
            }
        }
    }

    /**
     * Validates each entry in a list against a list of custom Validators. Each validator in the list of validators must inherit or be an
     * instance of Validator class
     */
    public static class ListEntryCustomValidator extends Validator {

        private Class<?>[] entryValidators;

        public ListEntryCustomValidator(Map<String, Object> params) {
            this.entryValidators = (Class<?>[]) params.get(ConfigValidationAnnotations.ValidatorParams.ENTRY_VALIDATOR_CLASSES);
        }

        public static void validateField(String name, Class<?>[] validators, Object o)
                throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
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
                                "validator: {} cannot be used in ListEntryCustomValidator.  Individual entry validators must a instance of " +
                                        "Validator class",
                                validator.getName());
                    }
                }
            }
        }

        @Override
        public void validateField(String name, Object o) {
            try {
                validateField(name, this.entryValidators, o);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @NoArgsConstructor
    public static class TopicNameValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            new StringValidator().validateField(name, o);
            String topic = (String) o;
            if (!TopicName.isValid(topic)) {
                throw new IllegalArgumentException(
                        String.format("The topic name %s is invalid for field '%s'", topic, name));
            }
        }
    }

    public static class WindowConfigValidator extends Validator{

        public static void validateWindowConfig(WindowConfig windowConfig) {
            if (windowConfig.getWindowLengthDurationMs() == null && windowConfig.getWindowLengthCount() == null) {
                throw new IllegalArgumentException("Window length is not specified");
            }

            if (windowConfig.getWindowLengthDurationMs() != null && windowConfig.getWindowLengthCount() != null) {
                throw new IllegalArgumentException(
                        "Window length for time and count are set! Please set one or the other.");
            }

            if (windowConfig.getWindowLengthCount() != null) {
                if (windowConfig.getWindowLengthCount() <= 0) {
                    throw new IllegalArgumentException(
                            "Window length must be positive [" + windowConfig.getWindowLengthCount() + "]");
                }
            }

            if (windowConfig.getWindowLengthDurationMs() != null) {
                if (windowConfig.getWindowLengthDurationMs() <= 0) {
                    throw new IllegalArgumentException(
                            "Window length must be positive [" + windowConfig.getWindowLengthDurationMs() + "]");
                }
            }

            if (windowConfig.getSlidingIntervalCount() != null) {
                if (windowConfig.getSlidingIntervalCount() <= 0) {
                    throw new IllegalArgumentException(
                            "Sliding interval must be positive [" + windowConfig.getSlidingIntervalCount() + "]");
                }
            }

            if (windowConfig.getSlidingIntervalDurationMs() != null) {
                if (windowConfig.getSlidingIntervalDurationMs() <= 0) {
                    throw new IllegalArgumentException(
                            "Sliding interval must be positive [" + windowConfig.getSlidingIntervalDurationMs() + "]");
                }
            }

            if (windowConfig.getTimestampExtractorClassName() != null) {
                if (windowConfig.getMaxLagMs() != null) {
                    if (windowConfig.getMaxLagMs() < 0) {
                        throw new IllegalArgumentException(
                                "Lag duration must be positive [" + windowConfig.getMaxLagMs() + "]");
                    }
                }
                if (windowConfig.getWatermarkEmitIntervalMs() != null) {
                    if (windowConfig.getWatermarkEmitIntervalMs() <= 0) {
                        throw new IllegalArgumentException(
                                "Watermark interval must be positive [" + windowConfig.getWatermarkEmitIntervalMs() + "]");
                    }
                }
            }
        }

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            if (!(o instanceof WindowConfig)) {
                throw new IllegalArgumentException(String.format("Field '%s' must be of WindowConfig type!", name));
            }
            WindowConfig windowConfig = (WindowConfig) o;
            validateWindowConfig(windowConfig);
        }
    }

    public static class SourceConfigValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            SourceConfig sourceConfig = (SourceConfig) o;
            String sourceClassName;
            try {
                sourceClassName = ConnectorUtils.getIOSourceClass(sourceConfig.getArchive());
            } catch (IOException e1) {
                throw new IllegalArgumentException("Failed to extract source class from archive", e1);
            }

            try (NarClassLoader clsLoader = NarClassLoader.getFromArchive(new File(sourceConfig.getArchive()),
                    Collections.emptySet())) {

                Class<?> typeArg = getSourceType(sourceClassName, clsLoader);
                String serdeClassname = sourceConfig.getSerdeClassName();

                if (StringUtils.isEmpty(serdeClassname)) {
                    serdeClassname = DefaultSerDe.class.getName();
                }

                try {
                    loadClass(serdeClassname);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException(String
                            .format("The input serialization/deserialization class %s does not exist", serdeClassname));
                }

                try {
                    new ValidatorImpls.ImplementsClassValidator(SerDe.class).validateField(name, serdeClassname);
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException(
                            String.format("The input serialization/deserialization class %s does not not implement %s",
                                    serdeClassname, SerDe.class.getCanonicalName()));
                }

                if (serdeClassname.equals(DefaultSerDe.class.getName())) {
                    if (!DefaultSerDe.IsSupportedType(typeArg)) {
                        throw new IllegalArgumentException("The default Serializer does not support type " + typeArg);
                    }
                } else {
                    SerDe serDe = (SerDe) Reflections.createInstance(serdeClassname, clsLoader);
                    if (serDe == null) {
                        throw new IllegalArgumentException(
                                String.format("The SerDe class %s does not exist", serdeClassname));
                    }
                    Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

                    // type inheritance information seems to be lost in generic type
                    // load the actual type class for verification
                    Class<?> fnInputClass;
                    Class<?> serdeInputClass;
                    try {
                        fnInputClass = Class.forName(typeArg.getName(), true, clsLoader);
                        // get output serde
                        serdeInputClass = Class.forName(serDeTypes[1].getName(), true, clsLoader);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Failed to load type class", e);
                    }

                    if (!fnInputClass.isAssignableFrom(serdeInputClass)) {
                        throw new IllegalArgumentException(
                                "Serializer type mismatch " + typeArg + " vs " + serDeTypes[1]);
                    }
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public static class SinkConfigValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            SinkConfig sinkConfig = (SinkConfig) o;
            // if function-pkg url is present eg: file://xyz.jar then admin-tool might not have access of the file at
            // the same location so, need to rely on server side validation.
            if (Utils.isFunctionPackageUrlSupported(sinkConfig.getArchive())) {
                return;
            }

            // make we sure we have one source of input
            if ((sinkConfig.getTopicToSerdeClassName() == null || sinkConfig.getTopicToSerdeClassName().isEmpty())
                    && isBlank(sinkConfig.getTopicsPattern())) {
                throw new IllegalArgumentException("Must specify at least one topic of input via inputs, " +
                        "customSerdeInputs, or topicPattern");
            }


            try (NarClassLoader clsLoader = NarClassLoader.getFromArchive(new File(sinkConfig.getArchive()),
                    Collections.emptySet())) {
                String sinkClassName = ConnectorUtils.getIOSinkClass(sinkConfig.getArchive());
                Class<?> typeArg = getSinkType(sinkClassName, clsLoader);

                if (sinkConfig.getTopicToSerdeClassName() != null) {
                    sinkConfig.getTopicToSerdeClassName().forEach((topicName, serdeClassname) -> {
                        if (StringUtils.isEmpty(serdeClassname)) {
                            serdeClassname = DefaultSerDe.class.getName();
                        }

                        try {
                            loadClass(serdeClassname);
                        } catch (ClassNotFoundException e) {
                            throw new IllegalArgumentException(String.format(
                                    "The input serialization/deserialization class %s does not exist", serdeClassname));
                        }

                        try {
                            new ValidatorImpls.ImplementsClassValidator(SerDe.class).validateField(name, serdeClassname);

                        } catch (IllegalArgumentException ex) {
                            throw new IllegalArgumentException(String.format(
                                    "The input serialization/deserialization class %s does not not " + "implement %s",
                                    serdeClassname, SerDe.class.getCanonicalName()));
                        }

                        if (serdeClassname.equals(DefaultSerDe.class.getName())) {
                            if (!DefaultSerDe.IsSupportedType(typeArg)) {
                                throw new IllegalArgumentException("The default Serializer does not support type " +
                                        typeArg);
                            }
                        } else {
                            SerDe serDe = (SerDe) Reflections.createInstance(serdeClassname, clsLoader);
                            if (serDe == null) {
                                throw new IllegalArgumentException(
                                        String.format("The SerDe class %s does not exist", serdeClassname));
                            }
                            Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

                            // type inheritance information seems to be lost in generic type
                            // load the actual type class for verification
                            Class<?> fnInputClass;
                            Class<?> serdeInputClass;
                            try {
                                fnInputClass = Class.forName(typeArg.getName(), true, clsLoader);
                                // get input serde
                                serdeInputClass = Class.forName(serDeTypes[0].getName(), true, clsLoader);
                            } catch (ClassNotFoundException e) {
                                throw new IllegalArgumentException("Failed to load type class", e);
                            }

                            if (!fnInputClass.isAssignableFrom(serdeInputClass)) {
                                throw new IllegalArgumentException(
                                        "Serializer type mismatch " + typeArg + " vs " + serDeTypes[0]);
                            }
                        }
                    });
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public static class FileValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            new StringValidator().validateField(name, o);

            String path = (String) o;

            if(!Utils.isFunctionPackageUrlSupported(path)) {
                // check file existence if path is not url and local path
                if (!fileExists(path)) {
                    throw new IllegalArgumentException
                            (String.format("File %s specified in field '%s' does not exist", path, name));
                }
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