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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Helper methods wrt Classloading.
 */
public class ClassLoaderUtils {
    /**
     * Load a jar.
     *
     * @param jar file of jar
     * @return classloader
     * @throws MalformedURLException
     */
    public static ClassLoader loadJar(File jar) throws MalformedURLException {
        java.net.URL url = jar.toURI().toURL();
        return AccessController.doPrivileged(
            (PrivilegedAction<URLClassLoader>) () -> new URLClassLoader(new URL[]{url}));
    }

    public static ClassLoader extractClassLoader(File packageFile) throws Exception {
        if (packageFile != null) {
            return loadJar(packageFile);
        }
        return null;
    }

    public static Class<?> loadClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
        Class<?> objectClass;
        try {
            objectClass = Class.forName(className);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            if (classLoader != null) {
                objectClass = classLoader.loadClass(className);
            } else {
                throw e;
            }
        }
        return objectClass;
    }

    public static void implementsClass(String className, Class<?> klass, ClassLoader classLoader) {
        Class<?> objectClass;
        try {
            objectClass = loadClass(className, classLoader);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new IllegalArgumentException("Cannot find/load class " + className);
        }

        if (!klass.isAssignableFrom(objectClass)) {
            throw new IllegalArgumentException(
                    String.format("%s does not implement %s", className, klass.getName()));
        }
    }
}
