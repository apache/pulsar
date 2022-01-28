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
package org.apache.pulsar.common.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utils related to reflections.
 */
public class Reflections {

    private static final Map<Class<?>, Constructor<?>> constructorCache =
        new ConcurrentHashMap<>();

    private static final Map PRIMITIVE_NAME_TYPE_MAP = new HashMap();

    static {
        PRIMITIVE_NAME_TYPE_MAP.put("boolean", Boolean.TYPE);
        PRIMITIVE_NAME_TYPE_MAP.put("byte", Byte.TYPE);
        PRIMITIVE_NAME_TYPE_MAP.put("char", Character.TYPE);
        PRIMITIVE_NAME_TYPE_MAP.put("short", Short.TYPE);
        PRIMITIVE_NAME_TYPE_MAP.put("int", Integer.TYPE);
        PRIMITIVE_NAME_TYPE_MAP.put("long", Long.TYPE);
        PRIMITIVE_NAME_TYPE_MAP.put("float", Float.TYPE);
        PRIMITIVE_NAME_TYPE_MAP.put("double", Double.TYPE);
    }

    /**
     * Create an instance of <code>userClassName</code> using provided <code>classLoader</code>.
     * This instance should implement the provided interface <code>xface</code>.
     *
     * @param userClassName user class name
     * @param xface the interface that the reflected instance should implement
     * @param classLoader class loader to load the class.
     * @return the instance
     */
    public static <T> T createInstance(String userClassName,
                                       Class<T> xface,
                                       ClassLoader classLoader) {
        Class<?> theCls;
        try {
            theCls = Class.forName(userClassName, true, classLoader);
        } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
            throw new RuntimeException("User class must be in class path", cnfe);
        }
        if (!xface.isAssignableFrom(theCls)) {
            throw new RuntimeException(userClassName + " does not implement " + xface.getName());
        }
        Class<T> tCls = (Class<T>) theCls.asSubclass(xface);
        T result;
        try {
            Constructor<T> meth = (Constructor<T>) constructorCache.get(theCls);
            if (null == meth) {
                meth = tCls.getDeclaredConstructor();
                meth.setAccessible(true);
                constructorCache.put(theCls, meth);
            }
            result = meth.newInstance();
        } catch (InstantiationException ie) {
            throw new RuntimeException("User class must be concrete", ie);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("User class must have a no-arg constructor", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("User class must have a public constructor", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("User class constructor throws exception", e);
        }
        return result;

    }

    /**
     * Create an instance of <code>userClassName</code> using provided <code>classLoader</code>.
     *
     * @param userClassName user class name
     * @param classLoader class loader to load the class.
     * @return the instance
     */
    public static Object createInstance(String userClassName,
                                        ClassLoader classLoader) {
        Class<?> theCls;
        try {
            theCls = Class.forName(userClassName, true, classLoader);
        } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
            throw new RuntimeException("User class must be in class path", cnfe);
        }
        Object result;
        try {
            Constructor<?> meth = constructorCache.get(theCls);
            if (null == meth) {
                meth = theCls.getDeclaredConstructor();
                meth.setAccessible(true);
                constructorCache.put(theCls, meth);
            }
            result = meth.newInstance();
        } catch (InstantiationException ie) {
            throw new RuntimeException("User class must be concrete", ie);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("User class doesn't have such method", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("User class must have a no-arg constructor", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("User class constructor throws exception", e);
        }
        return result;

    }

    public static Object createInstance(String userClassName,
                                        ClassLoader classLoader, Object[] params, Class[] paramTypes) {
        if (params.length != paramTypes.length) {
            throw new RuntimeException(
                    "Unequal number of params and paramTypes. Each param must have a corresponding param type");
        }
        Class<?> theCls;
        try {
            theCls = Class.forName(userClassName, true, classLoader);
        } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
            throw new RuntimeException("User class must be in class path", cnfe);
        }
        Object result;
        try {
            Constructor<?> meth = constructorCache.get(theCls);
            if (null == meth) {
                meth = theCls.getDeclaredConstructor(paramTypes);
                meth.setAccessible(true);
                constructorCache.put(theCls, meth);
            }
            result = meth.newInstance(params);
        } catch (InstantiationException ie) {
            throw new RuntimeException("User class must be concrete", ie);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("User class doesn't have such method", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("User class must have a no-arg constructor", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("User class constructor throws exception", e);
        }
        return result;

    }

    public static Object createInstance(String userClassName, java.io.File jar) {
        try {
            return createInstance(userClassName, ClassLoaderUtils.loadJar(jar));
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Check if a class is in a jar.
     *
     * @param jar location of the jar
     * @param fqcn fully qualified class name to search for in jar
     * @return true if class can be loaded from jar and false if otherwise
     */
    public static boolean classExistsInJar(java.io.File jar, String fqcn) {
        java.net.URLClassLoader loader = null;
        try {
            loader = (URLClassLoader) ClassLoaderUtils.loadJar(jar);
            Class.forName(fqcn, false, loader);
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError | IOException e) {
            return false;
        } finally {
            if (loader != null) {
                try {
                    loader.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    /**
     * Check if class exists.
     *
     * @param fqcn fully qualified class name to search for
     * @return true if class can be loaded from jar and false if otherwise
     */
    public static boolean classExists(String fqcn) {
        try {
            Class.forName(fqcn);
            return true;
        } catch (ClassNotFoundException e) {
           return false;
        }
    }

    /**
     * check if a class implements an interface.
     *
     * @param fqcn fully qualified class name to search for in jar
     * @param xface interface to check if implement
     * @return true if class from jar implements interface xface and false if otherwise
     */
    public static boolean classInJarImplementsIface(java.io.File jar, String fqcn, Class xface) {
        boolean ret = false;
        java.net.URLClassLoader loader = null;
        try {
            loader = (URLClassLoader) ClassLoaderUtils.loadJar(jar);
            if (xface.isAssignableFrom(Class.forName(fqcn, false, loader))){
                ret = true;
            }
        } catch (ClassNotFoundException | NoClassDefFoundError | IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (loader != null) {
                try {
                    loader.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return ret;
    }

    /**
     * check if class implements interface.
     *
     * @param fqcn fully qualified class name
     * @param xface the interface the fqcn should implement
     * @return true if class implements interface xface and false if otherwise
     */
    public static boolean classImplementsIface(String fqcn, Class xface) {
        boolean ret = false;
        try {
            if (xface.isAssignableFrom(Class.forName(fqcn))){
                ret = true;
            }
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    private static boolean isPrimitive(String type) {
        return PRIMITIVE_NAME_TYPE_MAP.containsKey(type);
    }

    /**
     * Load class to resolve array types.
     *
     * @param className class name
     * @param classLoader class loader
     * @return loaded class
     * @throws ClassNotFoundException
     */
    public static Class loadClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
        if (className.length() == 1) {
            char type = className.charAt(0);
            if (type == 'B') {
                return Byte.TYPE;
            } else if (type == 'C') {
                return Character.TYPE;
            } else if (type == 'D') {
                return Double.TYPE;
            } else if (type == 'F') {
                return Float.TYPE;
            } else if (type == 'I') {
                return Integer.TYPE;
            } else if (type == 'J') {
                return Long.TYPE;
            } else if (type == 'S') {
                return Short.TYPE;
            } else if (type == 'Z') {
                return Boolean.TYPE;
            } else if (type == 'V') {
                return Void.TYPE;
            } else {
                throw new ClassNotFoundException(className);
            }
        } else if (isPrimitive(className)) {
            return (Class) PRIMITIVE_NAME_TYPE_MAP.get(className);
        } else if (className.charAt(0) == 'L' && className.charAt(className.length() - 1) == ';') {
            return classLoader.loadClass(className.substring(1, className.length() - 1));
        } else {
            try {
                return classLoader.loadClass(className);
            } catch (ClassNotFoundException | NoClassDefFoundError var4) {
                if (className.charAt(0) != '[') {
                    throw var4;
                } else {
                    // CHECKSTYLE.OFF: EmptyStatement
                    int arrayDimension;
                    for (arrayDimension = 0; className.charAt(arrayDimension) == '['; ++arrayDimension) {
                    }
                    // CHECKSTYLE.ON: EmptyStatement

                    Class componentType = loadClass(className.substring(arrayDimension), classLoader);
                    return Array.newInstance(componentType, new int[arrayDimension]).getClass();
                }
            }
        }
    }

    public static List<Field> getAllFields(Class<?> type) {
        List<Field> fields = new LinkedList<>();
        fields.addAll(Arrays.asList(type.getDeclaredFields()));

        if (type.getSuperclass() != null) {
            fields.addAll(getAllFields(type.getSuperclass()));
        }

        return fields;
    }
}
