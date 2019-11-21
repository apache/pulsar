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
package org.apache.pulsar.functions.worker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * A starter to start function worker.
 */
@Slf4j
public class FunctionWorkerStarter {

    private static class WorkerArguments {
        @Parameter(
            names = { "-c", "--conf" },
            description = "Configuration File for Function Worker")
        private String configFile;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;
    }

    public static void main(String[] args) throws Exception {
        WorkerArguments workerArguments = new WorkerArguments();
        JCommander commander = new JCommander(workerArguments);
        commander.setProgramName("FunctionWorkerStarter");

        // parse args by commander
        commander.parse(args);

        if (workerArguments.help) {
            commander.usage();
            System.exit(-1);
            return;
        }

        WorkerConfig workerConfig;
        if (isBlank(workerArguments.configFile)) {
            workerConfig = new WorkerConfig();
        } else {
            workerConfig = WorkerConfig.load(workerArguments.configFile);
        }

        final Worker worker = new Worker(workerConfig);
        try {
            loadJdbcDriver(workerConfig.getConnectorsDirectory());
            worker.start();
        }catch(Exception e){
            log.error("Failed to start function worker", e);
            worker.stop();
            System.exit(-1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping function worker service ..");
            worker.stop();
        }));
    }
    private static void loadJdbcDriver(String connectorsDirectory) throws IOException {
        Path path = Paths.get(connectorsDirectory).toAbsolutePath();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.jar")) {
            for (Path archive : stream) {
                loadJar(archive.toFile().getPath());
            }
        }
    }
    private static void loadJar(String jarPath) {
        File jarFile = new File(jarPath);
        if(jarFile.exists() == false){
            return;
        }
        Method method = null;
        boolean accessible = false;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            URLClassLoader classLoader = (URLClassLoader) getClassLoader();
            accessible = method.isAccessible();
            if (accessible){
                method.setAccessible(true);
            }
            method.invoke(classLoader,jarFile.toURI().toURL());
        } catch (Exception e) {
            log.error("load jar error ..",e);
        }finally {
            if(method != null){
                method.setAccessible(accessible);
            }
        }
    }
    private static ClassLoader getClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        }
        catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = FunctionWorkerStarter.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() returning null indicates the bootstrap ClassLoader
                try {
                    cl = ClassLoader.getSystemClassLoader();
                }
                catch (Throwable ex) {
                    // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
                }
            }
        }
        return cl;
    }
}
