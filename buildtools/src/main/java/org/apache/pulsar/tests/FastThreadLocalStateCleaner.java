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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleanup Thread Local state attach to Netty's FastThreadLocal.
 *
 * This is not thread-safe, but that aspect is ignored.
 */
public final class FastThreadLocalStateCleaner {
    private static final Logger LOG = LoggerFactory.getLogger(FastThreadLocalStateCleaner.class);
    private static final ThreadLocal<?> SLOW_THREAD_LOCAL_MAP = lookupSlowThreadLocalMap();
    private static final Class<?> FAST_THREAD_LOCAL_CLASS;
    private static final Method GET_THREAD_LOCAL_MAP;
    private static final Field INDEXED_VARIABLES_FIELD;
    private static final Object UNSET_OBJECT;

    static {
        Class<?> clazz = null;
        Method getThreadLocalMapMethod = null;
        Field indexedVariablesField = null;
        Object unsetObject = null;
        if (SLOW_THREAD_LOCAL_MAP != null) {
            try {
                clazz = ClassUtils.getClass("io.netty.util.concurrent.FastThreadLocalThread");
                Class<?> internalThreadLocalMapClass =
                        ClassUtils.getClass("io.netty.util.internal.InternalThreadLocalMap");
                getThreadLocalMapMethod = MethodUtils
                        .getMatchingAccessibleMethod(clazz, "threadLocalMap");
                indexedVariablesField = FieldUtils.getDeclaredField(internalThreadLocalMapClass,
                        "indexedVariables", true);
                Field unsetField = FieldUtils.getField(internalThreadLocalMapClass, "UNSET");
                unsetObject = unsetField.get(null);
            } catch (ClassNotFoundException | IllegalAccessException e) {
                // ignore
                LOG.debug("Ignoring exception", e);
                clazz = null;
                getThreadLocalMapMethod = null;
                indexedVariablesField = null;
                unsetObject = null;
            }
        }
        FAST_THREAD_LOCAL_CLASS = clazz;
        GET_THREAD_LOCAL_MAP = getThreadLocalMapMethod;
        INDEXED_VARIABLES_FIELD = indexedVariablesField;
        UNSET_OBJECT = unsetObject;
    }

    private final Predicate<Object> valueFilter;

    private static ThreadLocal<?> lookupSlowThreadLocalMap() {
        try {
            Field slowThreadLocalMapField = FieldUtils.getDeclaredField(
                    ClassUtils.getClass("io.netty.util.internal.InternalThreadLocalMap"),
                    "slowThreadLocalMap", true);
            if (slowThreadLocalMapField != null) {
                return (ThreadLocal<?>) slowThreadLocalMapField.get(null);
            } else {
                LOG.warn("Cannot find InternalThreadLocalMap.slowThreadLocalMap field."
                        + " This might be due to using an unsupported netty-common version.");
                return null;
            }
        } catch (IllegalAccessException | ClassNotFoundException e) {
            LOG.warn("Cannot find InternalThreadLocalMap.slowThreadLocalMap thread local", e);
            return null;
        }
    }

    public FastThreadLocalStateCleaner(Predicate<Object> valueFilter) {
        this.valueFilter = valueFilter;
    }

    public void cleanupAllFastThreadLocals(Thread thread, BiConsumer<Thread, Object> cleanedValueListener) {
        Objects.nonNull(thread);
        try {
            Object internalThreadLocalMap;
            if (FAST_THREAD_LOCAL_CLASS.isInstance(thread)) {
                internalThreadLocalMap = GET_THREAD_LOCAL_MAP.invoke(thread);
            } else {
                internalThreadLocalMap = ThreadLocalStateCleaner.INSTANCE
                        .getThreadLocalValue(SLOW_THREAD_LOCAL_MAP, thread);
            }
            if (internalThreadLocalMap != null) {
                Object[] indexedVariables = (Object[]) INDEXED_VARIABLES_FIELD.get(internalThreadLocalMap);
                for (int i = 0; i < indexedVariables.length; i++) {
                    Object threadLocalValue = indexedVariables[i];
                    if (threadLocalValue != UNSET_OBJECT && threadLocalValue != null) {
                        if (valueFilter.test(threadLocalValue)) {
                            indexedVariables[i] = UNSET_OBJECT;
                            if (cleanedValueListener != null) {
                                cleanedValueListener.accept(thread, threadLocalValue);
                            }
                        }
                    }
                }
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOG.warn("Cannot reset state for FastLocalThread {}", thread, e);
        }
    }

    // cleanup all fast thread local state on all active threads
    public void cleanupAllFastThreadLocals(BiConsumer<Thread, Object> cleanedValueListener) {
        for (Thread thread : ThreadUtils.getAllThreads()) {
            cleanupAllFastThreadLocals(thread, cleanedValueListener);
        }
    }

    public static boolean isEnabled() {
        return SLOW_THREAD_LOCAL_MAP != null && FAST_THREAD_LOCAL_CLASS != null;
    }
}
