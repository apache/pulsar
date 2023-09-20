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
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.mockito.internal.stubbing.InvocationContainerImpl;
import org.mockito.internal.util.MockUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleanup Mockito's Thread Local state. This is needed when Mockito has been used in an invalid way.
 * Mockito.reset method should be called at the end of a test in the same thread where the methods were
 * mocked/stubbed.
 */
public final class MockitoThreadLocalStateCleaner {
    public static final MockitoThreadLocalStateCleaner INSTANCE = new MockitoThreadLocalStateCleaner();
    private static final Logger LOG = LoggerFactory.getLogger(MockitoThreadLocalStateCleaner.class);
    private static final ThreadLocal<?> MOCKING_PROGRESS_PROVIDER = lookupMockingProgressThreadLocal();

    private static ThreadLocal<?> lookupMockingProgressThreadLocal() {
        try {
            Field profilerField = FieldUtils.getDeclaredField(
                    ClassUtils.getClass("org.mockito.internal.progress.ThreadSafeMockingProgress"),
                    "MOCKING_PROGRESS_PROVIDER", true);
            if (profilerField != null) {
                return (ThreadLocal<?>) profilerField.get(null);
            } else {
                LOG.warn("Cannot find Mockito's ThreadSafeMockingProgress.MOCKING_PROGRESS_PROVIDER field."
                        + " This might be due to using an unsupported Mockito version.");
                return null;
            }
        } catch (IllegalAccessException | ClassNotFoundException e) {
            LOG.warn("Cannot find Mockito's ThreadSafeMockingProgress.MOCKING_PROGRESS_PROVIDER thread local", e);
            return null;
        }
    }

    // force singleton
    private MockitoThreadLocalStateCleaner() {

    }

    public void cleanup() {
        ThreadLocalStateCleaner.INSTANCE.cleanupThreadLocal(MOCKING_PROGRESS_PROVIDER, (thread, mockingProgress) -> {
            try {
                LOG.info("Removing {} instance from thread {}", mockingProgress.getClass().getName(), thread);
                LOG.info("Calling MockingProgress.validateState() method on instance (toString={})", mockingProgress);
                MethodUtils.invokeMethod(mockingProgress, "validateState");
                Object ongoingStubbing = MethodUtils.invokeMethod(mockingProgress, "pullOngoingStubbing");
                if (ongoingStubbing != null) {
                    Object mock = MethodUtils.invokeMethod(ongoingStubbing, "getMock");
                    if (mock != null && MockUtil.isMock(mock)) {
                        LOG.warn("Invalid usage of Mockito detected on thread {}."
                                        + " There is ongoing stubbing on mock of class={} instance={}",
                                thread, mock.getClass().getName(), mock);
                        try {
                            clearInvocations(thread, mock);
                        } catch (Exception e) {
                            LOG.warn("Clearing invocations failed", e);
                        }
                    }
                }
            } catch (NoSuchMethodException | IllegalAccessException e) {
                LOG.debug("Cannot call validateState on existing Mockito ProgressProvider");
            } catch (InvocationTargetException e) {
                LOG.warn("Invalid usage of Mockito detected on thread {}", thread, e.getCause());
            } catch (Exception e) {
                LOG.warn("Removing {} instance from thread {} failed", mockingProgress.getClass().getName(), thread, e);
            }
        });
    }

    private static void clearInvocations(Thread thread, Object mock) {
        InvocationContainerImpl invocationContainer = MockUtil.getInvocationContainer(mock);
        if (invocationContainer.hasInvocationForPotentialStubbing()) {
            LOG.warn("Mock contains registered invocations that should be cleared. thread {} class={} "
                            + "instance={}",
                    thread, mock.getClass().getName(), mock);
            invocationContainer.clearInvocations();
        }
    }

    public boolean isEnabled() {
        return MOCKING_PROGRESS_PROVIDER != null;
    }
}
