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

import java.util.List;
import lombok.CustomLog;
import org.testng.ITestClass;

/**
 * Cleanup Thread Local state attach to Netty's FastThreadLocal.
 */
@CustomLog
public class FastThreadLocalCleanupListener extends BetweenTestClassesListenerAdapter {
    private static final boolean FAST_THREAD_LOCAL_CLEANUP_ENABLED =
            Boolean.parseBoolean(System.getProperty("testFastThreadLocalCleanup", "true"));
    private static final String FAST_THREAD_LOCAL_CLEANUP_PACKAGE =
            System.getProperty("testFastThreadLocalCleanupPackage", "org.apache.pulsar");
    private static final FastThreadLocalStateCleaner CLEANER = new FastThreadLocalStateCleaner(object -> {
        if ("*".equals(FAST_THREAD_LOCAL_CLEANUP_PACKAGE)) {
            return true;
        }
        Class<?> clazz = object.getClass();
        if (clazz.isArray()) {
            clazz = clazz.getComponentType();
        }
        Package pkg = clazz.getPackage();
        if (pkg != null && pkg.getName() != null) {
            return pkg.getName()
                    .startsWith(FAST_THREAD_LOCAL_CLEANUP_PACKAGE);
        } else {
            return false;
        }
    });

    @Override
    protected void onBetweenTestClasses(List<ITestClass> testClasses) {
        if (FAST_THREAD_LOCAL_CLEANUP_ENABLED && FastThreadLocalStateCleaner.isEnabled()) {
            log.info("Cleaning up FastThreadLocal thread local state.");
            CLEANER.cleanupAllFastThreadLocals((thread, value) -> {
                log.info().attr("thread", thread)
                        .attr("class", value.getClass().getName())
                        .attr("value", value)
                        .log("Cleaning FastThreadLocal state");
            });
        }
    }

}
