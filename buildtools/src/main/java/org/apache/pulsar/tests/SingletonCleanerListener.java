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
import java.util.List;
import lombok.CustomLog;
import org.apache.commons.lang3.ClassUtils;
import org.testng.ITestClass;

/**
 * This TestNG listener contains cleanup for some singletons or caches.
 */
@CustomLog
public class SingletonCleanerListener extends BetweenTestClassesListenerAdapter {
    private static final Method OBJECTMAPPERFACTORY_CLEARCACHES_METHOD;
    private static final Method JSONSCHEMA_CLEARCACHES_METHOD;

    static {
        Class<?> objectMapperFactoryClazz =
                null;
        try {
            objectMapperFactoryClazz = ClassUtils.getClass("org.apache.pulsar.common.util.ObjectMapperFactory");
        } catch (ClassNotFoundException e) {
            log.warn().exception(e).log("Cannot find ObjectMapperFactory class");
        }

        Method clearCachesMethod = null;
        try {
            if (objectMapperFactoryClazz != null) {
                clearCachesMethod =
                        objectMapperFactoryClazz
                                .getMethod("clearCaches");
            }
        } catch (NoSuchMethodException e) {
            log.warn().exception(e).log("Cannot find method for clearing singleton ObjectMapper caches");
        }
        OBJECTMAPPERFACTORY_CLEARCACHES_METHOD = clearCachesMethod;


        Class<?> jsonSchemaClazz = null;
        try {
            jsonSchemaClazz = ClassUtils.getClass("org.apache.pulsar.client.impl.schema.JSONSchema");
        } catch (ClassNotFoundException e) {
            log.warn().exception(e).log("Cannot find JSONSchema class");
        }

        Method jsonSchemaCleanCachesMethod = null;
        try {
            if (jsonSchemaClazz != null) {
                jsonSchemaCleanCachesMethod =
                        jsonSchemaClazz
                                .getMethod("clearCaches");
            }
        } catch (NoSuchMethodException e) {
            log.warn().exception(e).log("Cannot find method for clearing singleton JSONSchema caches");
        }
        JSONSCHEMA_CLEARCACHES_METHOD = jsonSchemaCleanCachesMethod;
    }

    @Override
    protected void onBetweenTestClasses(List<ITestClass> testClasses) {
        objectMapperFactoryClearCaches();
        jsonSchemaClearCaches();
    }

    // Call ObjectMapperFactory.clearCaches() using reflection to clear up classes held in
    // the singleton Jackson ObjectMapper instances
    private static void objectMapperFactoryClearCaches() {
        if (OBJECTMAPPERFACTORY_CLEARCACHES_METHOD != null) {
            try {
                OBJECTMAPPERFACTORY_CLEARCACHES_METHOD.invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.warn().exception(e).log("Cannot clean singleton ObjectMapper caches");
            }
        }
    }

    // Call JSONSchema.clearCaches() using reflection to clear up classes held in
    // the singleton Jackson ObjectMapper instance of JSONSchema class
    private static void jsonSchemaClearCaches() {
        if (JSONSCHEMA_CLEARCACHES_METHOD != null) {
            try {
                JSONSCHEMA_CLEARCACHES_METHOD.invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.warn().exception(e).log("Cannot clean singleton JSONSchema caches");
            }
        }
    }
}
