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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.BiConsumer;
import lombok.CustomLog;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

/**
 * Cleans up thread local state for all threads for a given thread local instance.
 */
@CustomLog
public final class ThreadLocalStateCleaner {
    public static final ThreadLocalStateCleaner INSTANCE = new ThreadLocalStateCleaner();
    private static final Method GET_THREADLOCAL_MAP_METHOD = MethodUtils
            .getMatchingMethod(ThreadLocal.class, "getMap", Thread.class);

    static {
        GET_THREADLOCAL_MAP_METHOD.setAccessible(true);
    }

    private volatile Method removeThreadlocalMethod;
    private volatile Method getThreadlocalEntryMethod;
    private volatile Field threadLocalEntryValueField;

    // enforce singleton
    private ThreadLocalStateCleaner() {

    }

    // use reflection to clear the state of the given thread local and thread
    public <T> void cleanupThreadLocal(ThreadLocal<?> threadLocal, Thread thread,
                                       BiConsumer<Thread, T> cleanedValueListener) {
        Objects.nonNull(threadLocal);
        Objects.nonNull(thread);
        try {
            Object threadLocalMap = GET_THREADLOCAL_MAP_METHOD.invoke(threadLocal, thread);
            if (threadLocalMap != null) {
                if (cleanedValueListener != null) {
                    callCleanedValueListener(threadLocal, thread, cleanedValueListener, threadLocalMap);
                }
                if (removeThreadlocalMethod == null) {
                    removeThreadlocalMethod = MethodUtils.getMatchingMethod(
                            threadLocalMap.getClass(), "remove", ThreadLocal.class);
                    removeThreadlocalMethod.setAccessible(true);
                }
                removeThreadlocalMethod.invoke(threadLocalMap, threadLocal);
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.warn().exception(e).log("Cannot cleanup thread local");
        }
    }

    private <T> void callCleanedValueListener(ThreadLocal<?> threadLocal, Thread thread,
                                              BiConsumer<Thread, T> cleanedValueListener, Object threadLocalMap)
            throws IllegalAccessException, InvocationTargetException {
        T currentValue = getCurrentValue(threadLocal, threadLocalMap);
        if (currentValue != null) {
            cleanedValueListener.accept(thread, currentValue);
        }
    }

    public <T> T getThreadLocalValue(ThreadLocal<?> threadLocal, Thread thread)
            throws InvocationTargetException, IllegalAccessException {
        Objects.nonNull(threadLocal);
        Objects.nonNull(thread);
        Object threadLocalMap = GET_THREADLOCAL_MAP_METHOD.invoke(threadLocal, thread);
        if (threadLocalMap != null) {
            return getCurrentValue(threadLocal, threadLocalMap);
        } else {
            return null;
        }
    }

    private <T> T getCurrentValue(ThreadLocal<?> threadLocal, Object threadLocalMap) throws IllegalAccessException,
            InvocationTargetException {
        if (getThreadlocalEntryMethod == null) {
            getThreadlocalEntryMethod = MethodUtils.getMatchingMethod(
                    threadLocalMap.getClass(), "getEntry", ThreadLocal.class);
            getThreadlocalEntryMethod.setAccessible(true);
        }
        Object entry = getThreadlocalEntryMethod.invoke(threadLocalMap, threadLocal);
        if (entry != null) {
            if (threadLocalEntryValueField == null) {
                threadLocalEntryValueField = FieldUtils.getField(entry.getClass(), "value",
                        true);
            }
            @SuppressWarnings("unchecked") // reflective access to ThreadLocal entry value
            T value = (T) threadLocalEntryValueField.get(entry);
            return value;
        }
        return null;
    }

    // cleanup thread local state on all active threads
    public <T> void cleanupThreadLocal(ThreadLocal<?> threadLocal, BiConsumer<Thread, T> cleanedValueListener) {
        Objects.nonNull(threadLocal);
        for (Thread thread : ThreadUtils.getAllThreads()) {
            cleanupThreadLocal(threadLocal, thread, cleanedValueListener);
        }
    }
}
