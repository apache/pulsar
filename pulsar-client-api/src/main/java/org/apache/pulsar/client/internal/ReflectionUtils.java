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
package org.apache.pulsar.client.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ReflectionUtils {

    private static final Map<String, Class<?>> loadedClasses = new ConcurrentHashMap<>();

    private static volatile ClassLoader classLoader = ReflectionUtils.class.getClassLoader();

    /**
     * Change the classloader to be used for loading Pulsar client implementation classes
     * @param newClassLoader
     */
    public static void setClassLoader(ClassLoader newClassLoader) {
        classLoader = newClassLoader;
    }

    public interface SupplierWithException<T> {
        T get() throws Exception;
    }

    public static <T> T catchExceptions(SupplierWithException<T> s) {
        try {
            return s.get();
        } catch (Throwable t) {
            if (t instanceof InvocationTargetException) {
                // exception is thrown during invocation
                Throwable cause = t.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
            throw new RuntimeException(t);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> newClassInstance(String className) {
        return (Class<T>) loadedClasses.computeIfAbsent(className, name -> {
            try {
                return Class.forName(className, true, classLoader);
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static <T> Constructor<T> getConstructor(String className, Class<?>... argTypes) {
        try {
            Class<T> clazz = newClassInstance(className);
            return clazz.getConstructor(argTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Method getStaticMethod(String className, String method, Class<?>... argTypes) {
        try {
            Class<T> clazz = newClassInstance(className);
            return clazz.getMethod(method, argTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newBuilder(String className) {
        return catchExceptions(
                () -> (T) ReflectionUtils.getStaticMethod(
                        className, "builder", null)
                        .invoke(null, null));
    }
}
