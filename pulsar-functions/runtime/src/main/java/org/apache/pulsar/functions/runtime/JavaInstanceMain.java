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

package org.apache.pulsar.functions.runtime;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;

public class JavaInstanceMain {

    public JavaInstanceMain() { }

    public static void main(String[] args) throws Exception {



        File functionApiJar = new File("/Users/jerrypeng/.m2/repository/org/apache/pulsar/pulsar-functions-api/2.4.0-SNAPSHOT/pulsar-functions-api-2.4.0-SNAPSHOT.jar");
        File pulsarClientApiJar = new File("/Users/jerrypeng/.m2/repository/org/apache/pulsar/pulsar-client-api/2.4.0-SNAPSHOT/pulsar-client-api-2.4.0-SNAPSHOT.jar");
//        File functionInstanceJar = new File("/Users/jerrypeng/.m2/repository/org/apache/pulsar/pulsar-functions-instance/2.4.0-SNAPSHOT/pulsar-functions-instance-2.4.0-SNAPSHOT.jar");
        File log4jCoreJar = new File("/Users/jerrypeng/.m2/repository/org/apache/logging/log4j/log4j-core/2.11.2/log4j-core-2.11.2.jar");
        File log4jApiJar = new File("/Users/jerrypeng/.m2/repository/org/apache/logging/log4j/log4j-api/2.11.2/log4j-api-2.11.2.jar");
        File log4jSlf4jImplJar = new File("/Users/jerrypeng/.m2/repository/org/apache/logging/log4j/log4j-slf4j-impl/2.11.2/log4j-slf4j-impl-2.11.2.jar");
        File sl4fjApiJar = new File("/Users/jerrypeng/.m2/repository/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar");
        ClassLoader root = loadJar(ClassLoader.getSystemClassLoader().getParent(),
                new File[]{functionApiJar, pulsarClientApiJar, sl4fjApiJar, log4jSlf4jImplJar, log4jApiJar, log4jCoreJar});

        File classpath = new File("/Users/jerrypeng/workspace/incubator-pulsar/distribution/server/target/classpath.txt");
        List<File> files = new LinkedList<>();
        for (String str: Files.readAllLines(classpath.toPath())) {
            for (String jar: str.split(":")) {
                files.add(new File(jar));
            }
        }
        ClassLoader functionFrameworkClsLoader = loadJar(root, files.toArray(new File[files.size()]));


        Object main = createInstance(JavaInstanceStarter.class.getName(), functionFrameworkClsLoader);

//        log.info("env: {}", System.getenv().toString());

        Method method = main.getClass().getDeclaredMethod("start", String[].class, ClassLoader.class, ClassLoader.class);

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
