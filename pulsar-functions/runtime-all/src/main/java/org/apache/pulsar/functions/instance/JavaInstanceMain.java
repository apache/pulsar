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
import java.util.LinkedList;
import java.util.List;

/**
 * This is the initial class that gets called when starting a Java Function instance.
 * Multiple class loaders are used to separate function framework dependencies from user code dependencies
 * This class will create three classloaders:
 *      1. The root classloader that will share interfaces between the function framework
 *      classloader and user code classloader. This classloader will contain the following dependencies
 *          - pulsar-functions-api
 *          - pulsar-client-api
 *          - log4j-slf4j-impl
 *          - slf4j-api
 *          - log4j-core
 *          - log4j-api
 *
 *      2. The Function framework classloader, a child of the root classloader, that loads all pulsar broker/worker dependencies
 *      3. The user code classloader, a child of the root classloader, that loads all user code dependencies
 *
 * This class should not use any other dependencies!
 *
 */
public class JavaInstanceMain {

    private static final String FUNCTIONS_FRAMEWORK_CLASSPATH = "pulsar.functions.framework.classpath";

    public JavaInstanceMain() { }

    public static void main(String[] args) throws Exception {

        // Set root classloader to current classpath
        ClassLoader root = Thread.currentThread().getContextClassLoader();

        // Get classpath for framework
        String framework_classpath = System.getProperty(FUNCTIONS_FRAMEWORK_CLASSPATH);
        assert framework_classpath != null;

        List<File> files = new LinkedList<>();
        for (String entry: framework_classpath.split(":")) {
            files.add(new File(entry));
        }

        ClassLoader functionFrameworkClsLoader = loadJar(root, files.toArray(new File[files.size()]));

        System.out.println("Using function root classloader: " + root);
        System.out.println("Using function framework classloader: " + functionFrameworkClsLoader);

        // use the function framework classloader to create org.apache.pulsar.functions.runtime.JavaInstanceStarter
        Object main = createInstance("org.apache.pulsar.functions.runtime.JavaInstanceStarter", functionFrameworkClsLoader);

        // Invoke start method of JavaInstanceStarter to start the framework code
        Method method = main.getClass().getDeclaredMethod("start", String[].class, ClassLoader.class, ClassLoader.class);

        System.out.println("Starting function instance...");
        method.invoke(main, args, functionFrameworkClsLoader, root);
    }

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
            Constructor<?> meth = theCls.getDeclaredConstructor();
            meth.setAccessible(true);

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

    public static ClassLoader loadJar(ClassLoader parent, File[] jars) throws MalformedURLException {
        URL[] urls = new URL[jars.length];
        for (int i = 0; i < jars.length; i++) {
            urls[i] = jars[i].toURI().toURL();
        }
        return new URLClassLoader(urls, parent);
    }
}
