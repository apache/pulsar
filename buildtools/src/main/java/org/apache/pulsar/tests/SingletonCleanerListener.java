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
    private static final Method CLASSSECURITYVALIDATOR_SETGLOBAL_METHOD;
    private static final Object INITIAL_CLASSSECURITYVALIDATOR;
    private static final Method AVROTRUSTEDCLASSES_ISINSTALLED_METHOD;

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

        Method setGlobalMethod = null;
        Object initialValidator = null;
        try {
            Class<?> validatorClazz = ClassUtils.getClass("org.apache.avro.util.ClassSecurityValidator");
            Class<?> predicateClazz =
                    ClassUtils.getClass("org.apache.avro.util.ClassSecurityValidator$ClassSecurityPredicate");
            // Captured before any test can install its own validator, so this is the pristine value.
            initialValidator = validatorClazz.getMethod("getGlobal").invoke(null);
            setGlobalMethod = validatorClazz.getMethod("setGlobal", predicateClazz);
        } catch (ClassNotFoundException e) {
            // Avro is not on the test classpath of every module; nothing to restore in that case.
            log.debug().exception(e).log("Avro ClassSecurityValidator not present, skipping validator reset");
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            log.warn().exception(e).log("Cannot access Avro's global class security validator");
        }
        CLASSSECURITYVALIDATOR_SETGLOBAL_METHOD = setGlobalMethod;
        INITIAL_CLASSSECURITYVALIDATOR = initialValidator;

        Method isInstalledMethod = null;
        try {
            isInstalledMethod =
                    ClassUtils.getClass("org.apache.pulsar.client.schema.AvroTrustedClasses")
                            .getMethod("isInstalled");
        } catch (ClassNotFoundException e) {
            log.debug().exception(e).log("AvroTrustedClasses not present");
        } catch (NoSuchMethodException e) {
            log.warn().exception(e).log("Cannot find AvroTrustedClasses.isInstalled()");
        }
        AVROTRUSTEDCLASSES_ISINSTALLED_METHOD = isInstalledMethod;
    }

    @Override
    protected void onBetweenTestClasses(List<ITestClass> testClasses) {
        objectMapperFactoryClearCaches();
        jsonSchemaClearCaches();
        restoreAvroClassSecurityValidator();
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

    // Avro's trusted-class validator is a JVM-global singleton, so a test class that installs its own
    // (directly, or via AvroTrustedClasses) would otherwise leak that trust into
    // every later test class in the same fork. Restore the value captured before any test ran.
    //
    // Pulsar's own validator is deliberately left in place: a broker shared across test classes stays
    // running and still needs to read and write its Avro-serialized system topics.
    private static void restoreAvroClassSecurityValidator() {
        if (CLASSSECURITYVALIDATOR_SETGLOBAL_METHOD == null || INITIAL_CLASSSECURITYVALIDATOR == null
                || isAvroTrustedClassesInstalled()) {
            return;
        }
        try {
            CLASSSECURITYVALIDATOR_SETGLOBAL_METHOD.invoke(null, INITIAL_CLASSSECURITYVALIDATOR);
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.warn().exception(e).log("Cannot restore Avro's global class security validator");
        }
    }

    private static boolean isAvroTrustedClassesInstalled() {
        if (AVROTRUSTEDCLASSES_ISINSTALLED_METHOD == null) {
            return false;
        }
        try {
            return (Boolean) AVROTRUSTEDCLASSES_ISINSTALLED_METHOD.invoke(null);
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.warn().exception(e).log("Cannot check whether Pulsar's Avro validator is installed");
            return false;
        }
    }
}
