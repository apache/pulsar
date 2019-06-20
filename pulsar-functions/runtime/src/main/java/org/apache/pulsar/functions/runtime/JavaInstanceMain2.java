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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.StringConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceMain2 {
    @Parameter(names = "--function_details", description = "Function details json\n", required = true)
    public String functionDetailsJsonString;
    @Parameter(
            names = "--jar",
            description = "Path to Jar\n",
            listConverter = StringConverter.class)
    public String jarFile;

    @Parameter(names = "--instance_id", description = "Instance Id\n", required = true)
    public int instanceId;

    @Parameter(names = "--function_id", description = "Function Id\n", required = true)
    public String functionId;

    @Parameter(names = "--function_version", description = "Function Version\n", required = true)
    public String functionVersion;

    @Parameter(names = "--pulsar_serviceurl", description = "Pulsar Service Url\n", required = true)
    public String pulsarServiceUrl;

    @Parameter(names = "--client_auth_plugin", description = "Client auth plugin name\n")
    public String clientAuthenticationPlugin;

    @Parameter(names = "--client_auth_params", description = "Client auth param\n")
    public String clientAuthenticationParameters;

    @Parameter(names = "--use_tls", description = "Use tls connection\n")
    public String useTls = Boolean.FALSE.toString();

    @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection\n")
    public String tlsAllowInsecureConnection = Boolean.TRUE.toString();

    @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification")
    public String tlsHostNameVerificationEnabled = Boolean.FALSE.toString();

    @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path")
    public String tlsTrustCertFilePath;

    @Parameter(names = "--state_storage_serviceurl", description = "State Storage Service Url\n", required= false)
    public String stateStorageServiceUrl;

    @Parameter(names = "--port", description = "Port to listen on\n", required = true)
    public int port;

    @Parameter(names = "--metrics_port", description = "Port metrics will be exposed on\n", required = true)
    public int metrics_port;

    @Parameter(names = "--max_buffered_tuples", description = "Maximum number of tuples to buffer\n", required = true)
    public int maxBufferedTuples;

    @Parameter(names = "--expected_healthcheck_interval", description = "Expected interval in seconds between healtchecks", required = true)
    public int expectedHealthCheckInterval;

    @Parameter(names = "--secrets_provider", description = "The classname of the secrets provider", required = false)
    public String secretsProviderClassName;

    @Parameter(names = "--secrets_provider_config", description = "The config that needs to be passed to secrets provider", required = false)
    public String secretsProviderConfig;

    @Parameter(names = "--cluster_name", description = "The name of the cluster this instance is running on", required = true)
    public String clusterName;


    public JavaInstanceMain2() { }

    public static void main(String[] args) throws Exception {
        Configurator.setRootLevel(Level.INFO);

        JavaInstanceMain2 javaInstanceMain = new JavaInstanceMain2();
        JCommander jcommander = new JCommander(javaInstanceMain);
        jcommander.setProgramName("JavaInstanceMain");

        // parse args by JCommander
        jcommander.parse(args);


        /*********/


        File functionApiJar = new File("/Users/jerrypeng/.m2/repository/org/apache/pulsar/pulsar-functions-api/2.4.0-SNAPSHOT/pulsar-functions-api-2.4.0-SNAPSHOT.jar");
        File pulsarClientApiJar = new File("/Users/jerrypeng/.m2/repository/org/apache/pulsar/pulsar-client-api/2.4.0-SNAPSHOT/pulsar-client-api-2.4.0-SNAPSHOT.jar");
        File log4jCoreJar = new File("/Users/jerrypeng/.m2/repository/org/apache/logging/log4j/log4j-core/2.11.2/log4j-core-2.11.2.jar");
        File log4jApiJar = new File("/Users/jerrypeng/.m2/repository/org/apache/logging/log4j/log4j-api/2.11.2/log4j-api-2.11.2.jar");
        File log4jSlf4jImplJar = new File("/Users/jerrypeng/.m2/repository/org/apache/logging/log4j/log4j-slf4j-impl/2.11.2/log4j-slf4j-impl-2.11.2.jar");
        File sl4fjApiJar = new File("/Users/jerrypeng/.m2/repository/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar");
        ClassLoader root = loadJar(ClassLoader.getSystemClassLoader().getParent(), new File[]{functionApiJar, pulsarClientApiJar, log4jCoreJar, log4jApiJar, log4jSlf4jImplJar, sl4fjApiJar});
//
//        File libDir = new File("/tmp/apache-pulsar-2.4.0-SNAPSHOT/lib");
//        List<File> l = new ArrayList<>(Arrays.asList(libDir.listFiles()));
//        ClassLoader functionFrameworkClsLoader = loadJar(root, l.toArray(new File[l.size()]));

        File classpath = new File("/Users/jerrypeng/workspace/incubator-pulsar/distribution/server/target/classpath.txt");
        List<File> files = new LinkedList<>();
        for (String str: Files.readAllLines(classpath.toPath())) {
            for (String jar: str.split(":")) {
                files.add(new File(jar));
            }
        }
        ClassLoader functionFrameworkClsLoader = loadJar(root, files.toArray(new File[files.size()]));

        File userFunctionJar = new File(javaInstanceMain.jarFile);
        ClassLoader userFunctionClsLoader = loadJar(root, new File[]{userFunctionJar});


        log.info("root: {} functionFrameworkClsLoader: {}  userFunctionClsLoader: {}", root, functionFrameworkClsLoader, userFunctionClsLoader);


        log.info("ori main: {} - {}",
                functionFrameworkClsLoader.loadClass("org.apache.pulsar.functions.runtime.JavaInstanceMain"),
                functionFrameworkClsLoader.loadClass("org.apache.pulsar.functions.runtime.JavaInstanceMain").getProtectionDomain().getCodeSource().getLocation());
        log.info(" - " +functionFrameworkClsLoader.loadClass("com.google.protobuf.Message$Builder").getProtectionDomain().getCodeSource().getLocation());

//        Object main = createInstance("org.apache.pulsar.functions.runtime.JavaInstanceMain", functionFrameworkClsLoader);
        Object main = functionFrameworkClsLoader.loadClass("org.apache.pulsar.functions.runtime.JavaInstanceMain").newInstance();

        log.info("env: {}", System.getenv().toString());
        log.info("fields: {} - {}", Arrays.asList(javaInstanceMain.getClass().getFields()), Arrays.asList(javaInstanceMain.getClass().getDeclaredFields()));
        for (Field field : javaInstanceMain.getClass().getFields()) {
            String name = field.getName();
            Object val =  field.get(javaInstanceMain);
            log.info(String.format("field: %s - %s", field.getName(), val));
            main.getClass().getField(name).set(main, val);

            log.info(String.format("set %s", main.getClass().getField(name).get(main)));
        }

        Method method = main.getClass().getDeclaredMethod("start", ClassLoader.class, ClassLoader.class);

        method.invoke(main, functionFrameworkClsLoader, userFunctionClsLoader);


        /*********/




//        javaInstanceMain.start();
    }

    public static ClassLoader loadJar(ClassLoader parent, File[] jars) throws MalformedURLException {
        URL[] urls = new URL[jars.length];
        for (int i = 0; i < jars.length; i++) {
            urls[i] = jars[i].toURI().toURL();
        }
        return new URLClassLoader(urls, parent);
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
}
