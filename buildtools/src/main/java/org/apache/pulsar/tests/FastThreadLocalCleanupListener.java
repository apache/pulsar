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
package org.apache.pulsar.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleanup Thread Local state attach to Netty's FastThreadLocal.
 */
public class FastThreadLocalCleanupListener extends BetweenTestClassesListenerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(FastThreadLocalCleanupListener.class);
    private static final boolean FAST_THREAD_LOCAL_CLEANUP_ENABLED =
            Boolean.valueOf(System.getProperty("testFastThreadLocalCleanup", "true"));
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
    protected void onBetweenTestClasses(Class<?> endedTestClass, Class<?> startedTestClass) {
        if (FAST_THREAD_LOCAL_CLEANUP_ENABLED && FastThreadLocalStateCleaner.isEnabled()) {
            LOG.info("Cleaning up FastThreadLocal thread local state.");
            CLEANER.cleanupAllFastThreadLocals((thread, value) -> {
                LOG.info("Cleaning FastThreadLocal state for thread {}, instance of class {}, value is {}", thread,
                        value.getClass().getName(), value);
            });
        }
    }

}
