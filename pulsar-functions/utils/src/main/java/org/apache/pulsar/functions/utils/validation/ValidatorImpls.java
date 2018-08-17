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

import static org.apache.pulsar.functions.utils.Utils.fileExists;
import static org.apache.pulsar.functions.utils.Utils.getSinkType;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Resources;
import org.apache.pulsar.functions.utils.SinkConfig;
import org.apache.pulsar.functions.utils.SourceConfig;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.utils.WindowConfig;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import net.jodah.typetools.TypeResolver;

@Slf4j
public class ValidatorImpls {

    private static final String DEFAULT_SERDE = "org.apache.pulsar.functions.api.utils.DefaultSerDe";

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

    @NoArgsConstructor
    public static class SchemaValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            new ValidatorImpls.ImplementsClassValidator(Schema.class).validateField(name, o);
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

            // inputs use default schema, so there is no check needed there

            // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
            // implements SerDe class
            if (functionConfig.getCustomSerdeInputs() != null) {
                functionConfig.getCustomSerdeInputs().forEach((topicName, inputSerializer) -> {
                    validateSerde(inputSerializer, typeArgs[0], name, clsLoader);
                });
            }

            // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
            // implements SerDe class
            if (functionConfig.getCustomSchemaInputs() != null) {
                functionConfig.getCustomSchemaInputs().forEach((topicName, schemaType) -> {
                    validateSchema(schemaType, typeArgs[0], name, clsLoader);
                });
            }

            // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
            // implements Schema or SerDe classes

            if (functionConfig.getInputSpecs() != null) {
                functionConfig.getInputSpecs().forEach((topicName, conf) -> {
                    // Need to make sure that one and only one of schema/serde is set
                    if ((conf.getSchemaType() != null && !conf.getSchemaType().isEmpty())
                            && (conf.getSerdeClassName() != null && !conf.getSerdeClassName().isEmpty())) {
                        throw new IllegalArgumentException(
                                String.format("Only one of schemaType or serdeClassName should be set in inputSpec"));
                    }
                    if (conf.getSerdeClassName() != null && !conf.getSerdeClassName().isEmpty()) {
                        validateSerde(conf.getSerdeClassName(), typeArgs[0], name, clsLoader);
                    }
                    if (conf.getSchemaType() != null && !conf.getSchemaType().isEmpty()) {
                        validateSchema(conf.getSchemaType(), typeArgs[0], name, clsLoader);
                    }
                });
            }

            if (Void.class.equals(typeArgs[1])) {
                return;
            }

            // One and only one of outputSchemaType and outputSerdeClassName should be set
            if ((functionConfig.getOutputSerdeClassName() != null && !functionConfig.getOutputSerdeClassName().isEmpty())
                    && (functionConfig.getOutputSchemaType()!= null && !functionConfig.getOutputSchemaType().isEmpty())) {
                throw new IllegalArgumentException(
                        String.format("Only one of outputSchemaType or outputSerdeClassName should be set"));
            }

            if (functionConfig.getOutputSchemaType() != null && !functionConfig.getOutputSchemaType().isEmpty()) {
                validateSchema(functionConfig.getOutputSchemaType(), typeArgs[1], name, clsLoader);
            }

            if (functionConfig.getOutputSerdeClassName() != null && !functionConfig.getOutputSerdeClassName().isEmpty()) {
                validateSerde(functionConfig.getOutputSerdeClassName(), typeArgs[1], name, clsLoader);
            }

        }

        private static void validateSchema(String schemaType, Class<?> typeArg, String name, ClassLoader clsLoader) {
            if (schemaType.isEmpty() || getBuiltinSchemaType(schemaType) != null) {
                // If it's empty, we use the default schema and no need to validate
                // If it's built-in, no need to validate
            } else {
                try {
                    new SchemaValidator().validateField(name, schemaType);
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException(
                            String.format("The input schema class %s does not not implement %s",
                                    schemaType, Schema.class.getCanonicalName()));
                }

                validateSchemaType(schemaType, typeArg, clsLoader);
            }
        }

        private static void validateSerde(String inputSerializer, Class<?> typeArg, String name, ClassLoader clsLoader) {
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

            if (inputSerializer.equals(DEFAULT_SERDE)) {
                // No checks needed here
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
                    fnInputClass = Class.forName(typeArg.getName(), true, clsLoader);
                    serdeInputClass = Class.forName(serDeTypes[0].getName(), true, clsLoader);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Failed to load type class", e);
                }

                if (!fnInputClass.isAssignableFrom(serdeInputClass)) {
                    throw new IllegalArgumentException("Serializer type mismatch " + typeArg + " vs " + serDeTypes[0]);
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

            if (functionConfig.getTopicsPattern() != null && !functionConfig.getTopicsPattern().isEmpty()) {
                throw new IllegalArgumentException("Topic-patterns is not supported for python runtime");
            }
            functionConfig.getInputSpecs().forEach((topic, conf) -> {
                if (conf.isRegexPattern()) {
                    throw new IllegalArgumentException("Topic-patterns is not supported for python runtime");
                }
            });
        }

        private static void verifyNoTopicClash(Collection<String> inputTopics, String outputTopic) throws IllegalArgumentException {
            if (inputTopics.contains(outputTopic)) {
                throw new IllegalArgumentException(
                        String.format("Output topic %s is also being used as an input topic (topics must be one or the other)",
                                outputTopic));
            }
        }

        private static void doCommonChecks(FunctionConfig functionConfig) {
            Collection<String> allInputTopics = collectAllInputTopics(functionConfig);
            if (allInputTopics.isEmpty()) {
                throw new RuntimeException("No input topic(s) specified for the function");
            }

            // Ensure that topics aren't being used as both input and output
            verifyNoTopicClash(allInputTopics, functionConfig.getOutput());

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

        private static Collection<String> collectAllInputTopics(FunctionConfig functionConfig) {
            List<String> retval = new LinkedList<>();
            if (functionConfig.getInputs() != null) {
                retval.addAll(functionConfig.getInputs());
            }
            if (functionConfig.getTopicsPattern() != null) {
                retval.add(functionConfig.getTopicsPattern());
            }
            if (functionConfig.getCustomSerdeInputs() != null) {
                retval.addAll(functionConfig.getCustomSerdeInputs().keySet());
            }
            if (functionConfig.getCustomSchemaInputs() != null) {
                retval.addAll(functionConfig.getCustomSchemaInputs().keySet());
            }
            if (functionConfig.getInputSpecs() != null) {
                retval.addAll(functionConfig.getInputSpecs().keySet());
            }
            return retval;
        }

        @Override
        public void validateField(String name, Object o) {
            FunctionConfig functionConfig = (FunctionConfig) o;
            doCommonChecks(functionConfig);
            if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
                if (!functionConfig.getJar().startsWith(Utils.FILE)) {
                    doJavaChecks(functionConfig, name);
                }
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
            if (sourceConfig.getArchive().startsWith(Utils.BUILTIN)) {
                // We don't have to check the archive, since it's provided on the worker itself
                return;
            }

            String sourceClassName;
            try {
                sourceClassName = ConnectorUtils.getIOSourceClass(sourceConfig.getArchive());
            } catch (IOException e1) {
                throw new IllegalArgumentException("Failed to extract source class from archive", e1);
            }

            try (NarClassLoader clsLoader = NarClassLoader.getFromArchive(new File(sourceConfig.getArchive()),
                    Collections.emptySet())) {
                Class<?> typeArg = getSourceType(sourceClassName, clsLoader);

                // Only one of serdeClassName or schemaType should be set
                if (sourceConfig.getSerdeClassName() != null && !sourceConfig.getSerdeClassName().isEmpty()
                        && sourceConfig.getSchemaType() != null && !sourceConfig.getSchemaType().isEmpty()) {
                    throw new IllegalArgumentException("Only one of serdeClassName or schemaType should be set");
                }

                if (sourceConfig.getSerdeClassName() != null && !sourceConfig.getSerdeClassName().isEmpty()) {
                    FunctionConfigValidator.validateSerde(sourceConfig.getSerdeClassName(),typeArg, name, clsLoader);
                }
                if (sourceConfig.getSchemaType() != null && !sourceConfig.getSchemaType().isEmpty()) {
                    FunctionConfigValidator.validateSchema(sourceConfig.getSchemaType(), typeArg, name, clsLoader);
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
            if (sinkConfig.getArchive().startsWith(Utils.BUILTIN)) {
                // We don't have to check the archive, since it's provided on the worker itself
                return;
            }

            // if function-pkg url is present eg: file://xyz.jar then admin-tool might not have access of the file at
            // the same location so, need to rely on server side validation.
            if (Utils.isFunctionPackageUrlSupported(sinkConfig.getArchive())) {
                return;
            }

            // make we sure we have one source of input
            if (collectAllInputTopics(sinkConfig).isEmpty()) {
                throw new IllegalArgumentException("Must specify at least one topic of input via topicToSerdeClassName, " +
                        "topicsPattern, topicToSchemaType or inputSpecs");
            }


            try (NarClassLoader clsLoader = NarClassLoader.getFromArchive(new File(sinkConfig.getArchive()),
                    Collections.emptySet())) {
                String sinkClassName = ConnectorUtils.getIOSinkClass(sinkConfig.getArchive());
                Class<?> typeArg = getSinkType(sinkClassName, clsLoader);

                if (sinkConfig.getTopicToSerdeClassName() != null) {
                    sinkConfig.getTopicToSerdeClassName().forEach((topicName, serdeClassName) -> {
                        FunctionConfigValidator.validateSerde(serdeClassName, typeArg, name, clsLoader);
                    });
                }

                if (sinkConfig.getTopicToSchemaType() != null) {
                    sinkConfig.getTopicToSchemaType().forEach((topicName, schemaType) -> {
                        FunctionConfigValidator.validateSchema(schemaType, typeArg, name, clsLoader);
                    });
                }

                // topicsPattern does not need checks

                if (sinkConfig.getInputSpecs() != null) {
                    sinkConfig.getInputSpecs().forEach((topicName, consumerSpec) -> {
                        // Only one is set
                        if (consumerSpec.getSerdeClassName() != null && !consumerSpec.getSerdeClassName().isEmpty()
                                && consumerSpec.getSchemaType() != null && !consumerSpec.getSchemaType().isEmpty()) {
                            throw new IllegalArgumentException("Only one of serdeClassName or schemaType should be set");
                        }
                        if (consumerSpec.getSerdeClassName() != null && !consumerSpec.getSerdeClassName().isEmpty()) {
                            FunctionConfigValidator.validateSerde(consumerSpec.getSerdeClassName(), typeArg, name, clsLoader);
                        }
                        if (consumerSpec.getSchemaType() != null && !consumerSpec.getSchemaType().isEmpty()) {
                            FunctionConfigValidator.validateSchema(consumerSpec.getSchemaType(), typeArg, name, clsLoader);
                        }
                    });
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        private static Collection<String> collectAllInputTopics(SinkConfig sinkConfig) {
            List<String> retval = new LinkedList<>();
            if (sinkConfig.getTopicToSerdeClassName() != null) {
                retval.addAll(sinkConfig.getTopicToSerdeClassName().keySet());
            }
            if (sinkConfig.getTopicsPattern() != null) {
                retval.add(sinkConfig.getTopicsPattern());
            }
            if (sinkConfig.getTopicToSchemaType() != null) {
                retval.addAll(sinkConfig.getTopicToSchemaType().keySet());
            }
            if (sinkConfig.getInputSpecs() != null) {
                retval.addAll(sinkConfig.getInputSpecs().keySet());
            }
            return retval;
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
                if (!path.startsWith(Utils.BUILTIN) && !fileExists(path)) {
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


    private static SchemaType getBuiltinSchemaType(String schemaTypeOrClassName) {
        try {
            return SchemaType.valueOf(schemaTypeOrClassName.toUpperCase());
        } catch (IllegalArgumentException e) {
            // schemaType is not referring to builtin type
            return null;
        }
    }

    private static void validateSchemaType(String scheamType, Class<?> typeArg, ClassLoader clsLoader) {
        validateCustomSchemaType(scheamType, typeArg, clsLoader);
    }

    private static void validateSerDeType(String serdeClassName, Class<?> typeArg, ClassLoader clsLoader) {
        SerDe<?> serDe = (SerDe<?>) Reflections.createInstance(serdeClassName, clsLoader);
        if (serDe == null) {
            throw new IllegalArgumentException(String.format("The SerDe class %s does not exist",
                    serdeClassName));
        }
        Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

        // type inheritance information seems to be lost in generic type
        // load the actual type class for verification
        Class<?> fnInputClass;
        Class<?> serdeInputClass;
        try {
            fnInputClass = Class.forName(typeArg.getName(), true, clsLoader);
            serdeInputClass = Class.forName(serDeTypes[0].getName(), true, clsLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to load type class", e);
        }

        if (!fnInputClass.isAssignableFrom(serdeInputClass)) {
            throw new IllegalArgumentException(
                    "Serializer type mismatch " + typeArg + " vs " + serDeTypes[0]);
        }
    }

    private static void validateCustomSchemaType(String schemaClassName, Class<?> typeArg, ClassLoader clsLoader) {
        Schema<?> schema = (Schema<?>) Reflections.createInstance(schemaClassName, clsLoader);
        if (schema == null) {
            throw new IllegalArgumentException(String.format("The Schema class %s does not exist",
                    schemaClassName));
        }
        Class<?>[] schemaTypes = TypeResolver.resolveRawArguments(Schema.class, schema.getClass());

        // type inheritance information seems to be lost in generic type
        // load the actual type class for verification
        Class<?> fnInputClass;
        Class<?> schemaInputClass;
        try {
            fnInputClass = Class.forName(typeArg.getName(), true, clsLoader);
            schemaInputClass = Class.forName(schemaTypes[0].getName(), true, clsLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Failed to load type class", e);
        }

        if (!fnInputClass.isAssignableFrom(schemaInputClass)) {
            throw new IllegalArgumentException(
                    "Schema type mismatch " + typeArg + " vs " + schemaTypes[0]);
        }
    }
}