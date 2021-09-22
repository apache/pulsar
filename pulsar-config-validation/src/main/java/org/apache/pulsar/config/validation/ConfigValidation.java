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


import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * The class that does the validation of all the members of a given object.
 */
public class ConfigValidation {

    private static final Class DEFAULT_ANNOTATION_CLASS = ConfigValidationAnnotations.class;

    /**
     * Validate the config object with annotations from annotationClass
     * @param config config object
     * @param annotationClass class with annotations to use
     */
    public static void validateConfig(Object config, Class annotationClass) {
        for (Field field : config.getClass().getDeclaredFields()) {
            Object value = null;
            field.setAccessible(true);
            try {
                value = field.get(config);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            validateField(field, value, annotationClass);
        }
        validateClass(config, annotationClass);
    }

    /**
     * Validate the config object with default annotation class
     * @param config config object
     */
    public static void validateConfig(Object config) {
        validateConfig(config, DEFAULT_ANNOTATION_CLASS);
    }

    private static void validateClass(Object config, Class annotationClass) {
        processAnnotations(config.getClass().getAnnotations(), config.getClass().getName(), config, annotationClass);
    }

    private static void validateField(Field field, Object value, Class annotationClass) {
        processAnnotations(field.getAnnotations(), field.getName(), value, annotationClass);
    }

    private static void processAnnotations(Annotation[] annotations, String fieldName, Object value, Class annotationClass) {
        try {
            for (Annotation annotation : annotations) {
                String type = annotation.annotationType().getName();
                Class<?> validatorClass = null;
                Class<?>[] classes = annotationClass.getDeclaredClasses();
                //check if annotation is one of our
                for (Class<?> clazz : classes) {
                    if (clazz.getName().equals(type)) {
                        validatorClass = clazz;
                        break;
                    }
                }
                if (validatorClass != null) {
                    Object v = validatorClass.cast(annotation);
                    @SuppressWarnings("unchecked")
                    Class<Validator> clazz = (Class<Validator>) validatorClass
                            .getMethod(ConfigValidationAnnotations.ValidatorParams.VALIDATOR_CLASS).invoke(v);
                    Validator o = null;
                    Map<String, Object> params = getParamsFromAnnotation(validatorClass, v);
                    //two constructor signatures used to initialize validators.
                    //One constructor takes input a Map of arguments, the other doesn't take any
                    //arguments (default constructor)
                    //If validator has a constructor that takes a Map as an argument call that constructor
                    if (hasConstructor(clazz, Map.class)) {
                        o = clazz.getConstructor(Map.class).newInstance(params);
                    } else { //If not call default constructor
                        o = clazz.getDeclaredConstructor().newInstance();
                    }
                    o.validateField(fieldName, value);
                }
            }
        } catch (NoSuchMethodException | IllegalAccessException
                | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
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