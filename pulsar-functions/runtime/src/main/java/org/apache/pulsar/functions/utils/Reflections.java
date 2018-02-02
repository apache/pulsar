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
package org.apache.pulsar.functions.utils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utils related to reflections.
 */
public class Reflections {

    private static final Map<Class<?>, Constructor<?>> constructorCache =
        new ConcurrentHashMap<>();

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
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("User class must be in class path", cnfe);
        }
        if (!xface.isAssignableFrom(theCls)) {
            throw new RuntimeException(userClassName + " not " + xface.getName());
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
            throw new RuntimeException("User class must a public constructor", e);
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
        } catch (ClassNotFoundException cnfe) {
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

    public static Object createInstance(String userClassName, java.io.File jar) {
        try {
            return createInstance(userClassName, loadJar(jar));
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Load a jar
     * @param jar file of jar
     * @return classloader
     * @throws MalformedURLException
     */
    public static ClassLoader loadJar(java.io.File jar) throws MalformedURLException {
        java.net.URL url = jar.toURI().toURL();
        return new URLClassLoader(new URL[]{url});
    }

    /**
     * Check if a class is in a jar
     * @param jar location of the jar
     * @param fqcn fully qualified class name to search for in jar
     * @return true if class can be loaded from jar and false if otherwise
     */
    public static boolean classExistsInJar(java.io.File jar, String fqcn) {
        try {
            java.net.URLClassLoader loader = (URLClassLoader) loadJar(jar);
            Class.forName(fqcn, false, loader);
            loader.close();
            return true;
        } catch (ClassNotFoundException | IOException e) {
            return false;
        }
    }

    /**
     * Check if class exists
     * @param fqcn fully qualified class name to search for
     * @return true if class can be loaded from jar and false if otherwise
     */
    public static boolean classExists(String fqcn) {
        try {
            Class.forName(fqcn);
            return true;
        } catch( ClassNotFoundException e ) {
           return false;
        }
    }

    /**
     * check if a class implements an interface
     * @param fqcn fully qualified class name to search for in jar
     * @param xface interface to check if implement
     * @return true if class from jar implements interface xface and false if otherwise
     */
    public static boolean classInJarImplementsIface(java.io.File jar, String fqcn, Class xface) {
        boolean ret = false;
        try {
            java.net.URLClassLoader loader = (URLClassLoader) loadJar(jar);
            if (xface.isAssignableFrom(Class.forName(fqcn, false, loader))){
                ret = true;
            }
            loader.close();
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    /**
     * check if class implements interface
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
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
}
