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

package org.apache.pulsar.functions.instance;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * This is the initial class that gets called when starting a Java Function instance.
 * Multiple class loaders are used to separate function instance dependencies from user code dependencies
 * This class will create three classloaders:
 *      1. The root classloader that will share interfaces between the function instance
 *      classloader and user code classloader. This classloader will contain the following dependencies
 *          - pulsar-io-core
 *          - pulsar-functions-api
 *          - pulsar-client-api
 *          - log4j-slf4j-impl
 *          - slf4j-api
 *          - log4j-core
 *          - log4j-api
 *
 *      2. The Function instance classloader, a child of the root classloader, that loads all pulsar broker/worker dependencies
 *      3. The user code classloader, a child of the root classloader, that loads all user code dependencies
 *
 * This class should not use any other dependencies!
 *
 */
public class JavaInstanceMain {

    private static final String FUNCTIONS_INSTANCE_CLASSPATH = "pulsar.functions.instance.classpath";

    public JavaInstanceMain() { }

    public static void main(String[] args) throws Exception {

        // Set root classloader to current classpath
        ClassLoader root = Thread.currentThread().getContextClassLoader();

        // Get classpath for function instance
        String functionInstanceClasspath = System.getProperty(FUNCTIONS_INSTANCE_CLASSPATH);
        if (functionInstanceClasspath == null) {
            throw new IllegalArgumentException("Property " + FUNCTIONS_INSTANCE_CLASSPATH + " is not set!");
        }

        List<File> files = new LinkedList<>();
        for (String entry: functionInstanceClasspath.split(":")) {
            if (isBlank(entry)) {
                continue;
            }
            // replace any asterisks i.e. wildcards as they don't work with url classloader
            File f = new File(entry.replace("*", ""));
            if (f.exists()) {
                if (f.isDirectory()) {
                    files.addAll(Arrays.asList(f.listFiles()));
                } else {
                    files.add(new File(entry));
                }
            } else {
                System.out.println(String.format("[WARN] %s on functions instance classpath does not exist", f.getAbsolutePath()));
            }
        }

        ClassLoader functionInstanceClsLoader = loadJar(root, files.toArray(new File[files.size()]));

        System.out.println("Using function root classloader: " + root);
        System.out.println("Using function instance classloader: " + functionInstanceClsLoader);

        // use the function instance classloader to create org.apache.pulsar.functions.runtime.JavaInstanceStarter
        Object main = createInstance("org.apache.pulsar.functions.runtime.JavaInstanceStarter", functionInstanceClsLoader);

        // Invoke start method of JavaInstanceStarter to start the function instance code
        Method method = main.getClass().getDeclaredMethod("start", String[].class, ClassLoader.class, ClassLoader.class);

        System.out.println("Starting function instance...");
        method.invoke(main, args, functionInstanceClsLoader, root);
    }

    public static Object createInstance(String userClassName,
                                        ClassLoader classLoader) {
        Class<?> theCls;
        try {
            theCls = Class.forName(userClassName, true, classLoader);
        } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
            throw new RuntimeException("Class " + userClassName + " must be in class path", cnfe);
        }
        Object result;
        try {
            Constructor<?> meth = theCls.getDeclaredConstructor();
            meth.setAccessible(true);

            result = meth.newInstance();
        } catch (InstantiationException ie) {
            throw new RuntimeException("User class must be concrete", ie);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Class " + userClassName + " doesn't have such method", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Class " + userClassName + " must have a no-arg constructor", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Class " + userClassName + " constructor throws exception", e);
        }
        return result;
    }

    public static ClassLoader loadJar(ClassLoader parent, File[] jars) throws MalformedURLException {
        URL[] urls = new URL[jars.length];
        for (int i = 0; i < jars.length; i++) {
            urls[i] = jars[i].toURI().toURL();
        }
        return new URLClassLoader(urls, parent);
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }
}
