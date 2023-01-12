/*
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

package org.apache.pulsar.tests;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This TestNG listener contains cleanup for some singletons or caches.
 */
public class SingletonCleanerListener extends BetweenTestClassesListenerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(SingletonCleanerListener.class);
    private static final Method OBJECTMAPPERFACTORY_CLEANCACHES_METHOD;

    static {
        Method method;
        try {
            method =
                    ClassUtils.getClass("org.apache.pulsar.common.util.ObjectMapperFactory")
                            .getMethod("clearCaches");
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            LOG.warn("Cannot find class or method for cleaning singleton ObjectMapper caches", e);
            method = null;
        }
        OBJECTMAPPERFACTORY_CLEANCACHES_METHOD = method;
    }

    @Override
    protected void onBetweenTestClasses(Class<?> endedTestClass, Class<?> startedTestClass) {
        cleanObjectMapperFactoryCaches();
    }

    // Call ObjectMapperFactory.clearCaches() using reflection to clear up classes held in
    // the singleton Jackson ObjectMapper instances
    private static void cleanObjectMapperFactoryCaches() {
        if (OBJECTMAPPERFACTORY_CLEANCACHES_METHOD != null) {
            try {
                OBJECTMAPPERFACTORY_CLEANCACHES_METHOD.invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                LOG.warn("Cannot clean singleton ObjectMapper caches", e);
            }
        }
    }
}
