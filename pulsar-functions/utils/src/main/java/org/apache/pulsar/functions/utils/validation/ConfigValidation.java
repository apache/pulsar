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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.utils.FunctionConfig;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.ValidatorParams.ACTUAL_RUNTIME;
import static org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.ValidatorParams.TARGET_RUNTIME;
import static org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.ValidatorParams.VALIDATOR_CLASS;

@Slf4j
public class ConfigValidation {

    public enum Runtime {
        ALL,
        JAVA,
        PYTHON
    }

    public static void validateConfig(Object config, String runtimeType, ClassLoader classLoader) {
        for (Field field : config.getClass().getDeclaredFields()) {
            Object value;
            field.setAccessible(true);
            try {
                value = field.get(config);
            } catch (IllegalAccessException e) {
               throw new RuntimeException(e);
            }
            validateField(field, value, Runtime.valueOf(runtimeType), classLoader);
        }
        validateClass(config, Runtime.valueOf(runtimeType), classLoader);
    }

    private static void validateClass(Object config, Runtime runtime, ClassLoader classLoader) {

        List<Annotation> annotationList = new LinkedList<>();
        Class<?>[] classes = ConfigValidationAnnotations.class.getDeclaredClasses();
        for (Class clazz : classes) {
            try {
                Annotation[] anots = config.getClass().getAnnotationsByType(clazz);
                annotationList.addAll(Arrays.asList(anots));
            } catch (ClassCastException e) {

            }
        }
        processAnnotations(annotationList, config.getClass().getName(), config, runtime, classLoader);
    }

    private static void validateField(Field field, Object value, Runtime runtime, ClassLoader classLoader) {
        List<Annotation> annotationList = new LinkedList<>();
        Class<?>[] classes = ConfigValidationAnnotations.class.getDeclaredClasses();
        for (Class clazz : classes) {
            try {
                Annotation[] anots = field.getAnnotationsByType(clazz);
                annotationList.addAll(Arrays.asList(anots));
            } catch (ClassCastException e) {

            }
        }
        processAnnotations(annotationList, field.getName(), value, runtime, classLoader);
    }

    private static void processAnnotations( List<Annotation> annotations, String fieldName, Object value,
                                           Runtime runtime, ClassLoader classLoader) {
        try {
            for (Annotation annotation : annotations) {

                String type = annotation.annotationType().getName();
                Class<?> validatorClass = null;
                Class<?>[] classes = ConfigValidationAnnotations.class.getDeclaredClasses();
                //check if annotation is one of our
                for (Class<?> clazz : classes) {
                    if (clazz.getName().equals(type)) {
                        validatorClass = clazz;
                        break;
                    }
                }
                if (validatorClass != null) {
                    Object v = validatorClass.cast(annotation);
                    if (hasMethod(validatorClass, VALIDATOR_CLASS)) {

                        @SuppressWarnings("unchecked")
                        Class<Validator> clazz = (Class<Validator>) validatorClass
                                .getMethod(ConfigValidationAnnotations.ValidatorParams.VALIDATOR_CLASS).invoke(v);
                        Validator o = null;
                        Map<String, Object> params = getParamsFromAnnotation(validatorClass, v);

                        if (params.containsKey(TARGET_RUNTIME)
                                && params.get(TARGET_RUNTIME) != Runtime.ALL
                                && params.get(TARGET_RUNTIME) != runtime) {
                            continue;
                        }
                        params.put(ACTUAL_RUNTIME, runtime);
                        //two constructor signatures used to initialize validators.
                        //One constructor takes input a Map of arguments, the other doesn't take any arguments (default constructor)

                        //If validator has a constructor that takes a Map as an argument call that constructor
                        if (hasConstructor(clazz, Map.class)) {
                            o = clazz.getConstructor(Map.class).newInstance(params);
                        } else { //If not call default constructor
                            o = clazz.newInstance();
                        }
                        o.validateField(fieldName, value, classLoader);
                    }
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean hasMethod(Class<?> clazz, String method) {
        try {
            clazz.getMethod(method);
            return true;
        } catch (NoSuchMethodException e) {
           return false;
        }
    }

    private static Map<String, Object> getParamsFromAnnotation(Class<?> validatorClass, Object v)
            throws InvocationTargetException, IllegalAccessException {
        Map<String, Object> params = new HashMap<String, Object>();
        for (Method method : validatorClass.getDeclaredMethods()) {

            Object value = null;
            try {
                value = (Object) method.invoke(v);
            } catch (IllegalArgumentException ex) {
                value = null;
            }
            if (value != null) {
                params.put(method.getName(), value);
            }
        }
        return params;
    }

    public static boolean hasConstructor(Class<?> clazz, Class<?> paramClass) {
        Class<?>[] classes = { paramClass };
        try {
            clazz.getConstructor(classes);
        } catch (NoSuchMethodException e) {
            return false;
        }
        return true;
    }
}
