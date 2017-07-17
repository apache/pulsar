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
package org.apache.pulsar.connect.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;

// Based off apache beam https://github.com/apache/beam
// org.apache.beam.sdk.util.InstanceBuilder.java class
public class InstanceBuilder<T> {

    /**
     * Create an InstanceBuilder for the given type.
     *
     * <p>The specified type is the type returned by {@link #build}, which is
     * typically the common base type or interface of the instance being
     * constructed.
     */
    public static <T> InstanceBuilder<T> ofType(Class<T> type) {
        return new InstanceBuilder<>(type);
    }

    /**
     * Sets the class name to be constructed.
     *
     * <p>If the name is a simple name (ie {@link Class#getSimpleName()}), then
     * the package of the return type is added as a prefix.
     *
     * <p>The default class is the return type, specified in {@link #ofType}.
     *
     * <p>Modifies and returns the {@code InstanceBuilder} for chaining.
     *
     * @throws ClassNotFoundException if no class can be found by the given name
     */
    public InstanceBuilder<T> fromClassName(String name) throws ClassNotFoundException {
        if (factoryClass != null) {
            throw new IllegalArgumentException("Class name may only be specified once");
        }

        if (name.indexOf('.') == -1) {
            name = type.getPackage().getName() + "." + name;
        }

        try {
            factoryClass = Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(
                    String.format("Could not find class: %s", name), e);
        }
        return this;
    }

    /**
     * Sets the factory class to use for instance construction.
     *
     * <p>Modifies and returns the {@code InstanceBuilder} for chaining.
     */
    public InstanceBuilder<T> fromClass(Class<?> factoryClass) {
        this.factoryClass = factoryClass;
        return this;
    }

    /**
     * Creates the instance by calling the factory method with the given
     * arguments.
     *
     * <h3>Defaults</h3>
     * <ul>
     *   <li>factory class: defaults to the output type class, overridden
     *   via {@link #fromClassName(String)}.
     *   <li>factory method: defaults to using a constructor on the factory
     *   class.
     * </ul>
     *
     * @throws RuntimeException if the method does not exist, on type mismatch,
     * or if the method cannot be made accessible.
     */
    public T build() {
        if (factoryClass == null) {
            factoryClass = type;
        }

        Class<?>[] types = parameterTypes
                .toArray(new Class<?>[parameterTypes.size()]);

        // TODO: cache results, to speed repeated type lookups?
        //if (methodName != null) {
        //    return buildFromMethod(types);
        //} else {
        return buildFromConstructor(types);
        //}
    }

    /////////////////////////////////////////////////////////////////////////////

    /**
     * Type of object to construct.
     */
    private final Class<T> type;

    /**
     * Types of parameters for Method lookup.
     *
     * @see Class#getDeclaredMethod(String, Class[])
     */
    private final List<Class<?>> parameterTypes = new LinkedList<>();


    private final List<Object> arguments = new LinkedList<>();

    /**
     * Name of factory method, or null to invoke the constructor.
     */
    private String methodName;

    /**
     * Factory class, or null to instantiate {@code type}.
     */
    private Class<?> factoryClass;

    private InstanceBuilder(Class<T> type) {
        this.type = type;
    }

    private T buildFromConstructor(Class<?>[] types) {
        if (factoryClass == null) {
            throw new IllegalStateException("factoryClass is null");
        }

        try {
            Constructor<?> constructor = factoryClass.getDeclaredConstructor(types);

            if (!type.isAssignableFrom(factoryClass)) {
                throw new IllegalStateException("Instance type " + factoryClass.getName()
                        + " must be assignable to " + type.getSimpleName());
            }

            if (!constructor.isAccessible()) {
                constructor.setAccessible(true);
            }

            Object[] args = arguments.toArray(new Object[arguments.size()]);
            return type.cast(constructor.newInstance(args));

        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to find constructor for "
                    + factoryClass.getName());

        } catch (InvocationTargetException
                | InstantiationException
                | IllegalAccessException e) {
            throw new RuntimeException("Failed to construct instance from "
                    + "constructor " + factoryClass.getName(), e);
        }
    }
}

