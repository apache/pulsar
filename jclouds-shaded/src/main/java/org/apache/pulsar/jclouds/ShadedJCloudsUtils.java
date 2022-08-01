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
package org.apache.pulsar.jclouds;

import com.google.inject.AbstractModule;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jclouds.ContextBuilder;
import org.jclouds.http.apachehc.config.ApacheHCHttpCommandExecutorServiceModule;
import org.jclouds.http.okhttp.config.OkHttpCommandExecutorServiceModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

import java.util.ArrayList;
import java.util.List;

/**
 * This utility class helps in dealing with shaded dependencies (especially Guice).
 */
@UtilityClass
@Slf4j
public class ShadedJCloudsUtils {

    /**
     * Use this System property to temporarily disable Apache Http Client Module.
     * If you encounter problems and decide to use this flag please
     * open a GH and share your problem.
     * Apache Http Client module should work well in all the environments.
     */
    private static final boolean ENABLE_APACHE_HC_MODULE = Boolean
            .parseBoolean(System.getProperty("pulsar.jclouds.use_apache_hc", "false"));
    private static final boolean ENABLE_OKHTTP_MODULE = Boolean
            .parseBoolean(System.getProperty("pulsar.jclouds.use_okhttp", "false"));
    static {
        log.info("Considering -Dpulsar.jclouds.use_apache_hc=" + ENABLE_APACHE_HC_MODULE);
        log.info("Considering -Dpulsar.jclouds.use_okhttp=" + ENABLE_OKHTTP_MODULE);
    }

    /**
     * Setup standard modules.
     * @param builder the build
     */
    public static void addStandardModules(ContextBuilder builder) {
        List<AbstractModule> modules = new ArrayList<>();
        modules.add(new SLF4JLoggingModule());
        if (ENABLE_OKHTTP_MODULE) {
            modules.add(new OkHttpCommandExecutorServiceModule());
        } else if (ENABLE_APACHE_HC_MODULE) {
            modules.add(new ApacheHCHttpCommandExecutorServiceModule());
        }
        builder.modules(modules);
    }

}
