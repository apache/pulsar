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
import lombok.CustomLog;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.mockito.internal.stubbing.InvocationContainerImpl;
import org.mockito.internal.util.MockUtil;

/**
 * Cleanup Mockito's Thread Local state. This is needed when Mockito has been used in an invalid way.
 * Mockito.reset method should be called at the end of a test in the same thread where the methods were
 * mocked/stubbed.
 */
@CustomLog
public final class MockitoThreadLocalStateCleaner {
    public static final MockitoThreadLocalStateCleaner INSTANCE = new MockitoThreadLocalStateCleaner();
    private static final ThreadLocal<?> MOCKING_PROGRESS_PROVIDER = lookupMockingProgressThreadLocal();

    private static ThreadLocal<?> lookupMockingProgressThreadLocal() {
        try {
            Field profilerField = FieldUtils.getDeclaredField(
                    ClassUtils.getClass("org.mockito.internal.progress.ThreadSafeMockingProgress"),
                    "MOCKING_PROGRESS_PROVIDER", true);
            if (profilerField != null) {
                return (ThreadLocal<?>) profilerField.get(null);
            } else {
                log.warn("Cannot find Mockito's ThreadSafeMockingProgress.MOCKING_PROGRESS_PROVIDER field."
                        + " This might be due to using an unsupported Mockito version.");
                return null;
            }
        } catch (IllegalAccessException | ClassNotFoundException e) {
            log.warn().exception(e)
                    .log("Cannot find Mockito's ThreadSafeMockingProgress.MOCKING_PROGRESS_PROVIDER thread local");
            return null;
        }
    }

    // force singleton
    private MockitoThreadLocalStateCleaner() {

    }

    public void cleanup() {
        ThreadLocalStateCleaner.INSTANCE.cleanupThreadLocal(MOCKING_PROGRESS_PROVIDER, (thread, mockingProgress) -> {
            try {
                log.info().attr("class", mockingProgress.getClass().getName())
                        .attr("thread", thread)
                        .log("Removing MockingProgress instance from thread");
                log.info().attr("instance", mockingProgress)
                        .log("Calling MockingProgress.validateState()");
                MethodUtils.invokeMethod(mockingProgress, "validateState");
                Object ongoingStubbing = MethodUtils.invokeMethod(mockingProgress, "pullOngoingStubbing");
                if (ongoingStubbing != null) {
                    Object mock = MethodUtils.invokeMethod(ongoingStubbing, "getMock");
                    if (mock != null && MockUtil.isMock(mock)) {
                        log.warn().attr("thread", thread)
                                .attr("mockClass", mock.getClass().getName())
                                .attr("mock", mock)
                                .log("Invalid usage of Mockito detected: ongoing stubbing on mock");
                        try {
                            clearInvocations(thread, mock);
                        } catch (Exception e) {
                            log.warn().exception(e).log("Clearing invocations failed");
                        }
                    }
                }
            } catch (NoSuchMethodException | IllegalAccessException e) {
                log.debug("Cannot call validateState on existing Mockito ProgressProvider");
            } catch (InvocationTargetException e) {
                log.warn().attr("thread", thread).exception(e.getCause())
                        .log("Invalid usage of Mockito detected");
            } catch (Exception e) {
                log.warn().attr("class", mockingProgress.getClass().getName())
                        .attr("thread", thread).exception(e)
                        .log("Removing MockingProgress instance from thread failed");
            }
        });
    }

    private static void clearInvocations(Thread thread, Object mock) {
        InvocationContainerImpl invocationContainer = MockUtil.getInvocationContainer(mock);
        if (invocationContainer.hasInvocationForPotentialStubbing()) {
            log.warn().attr("thread", thread)
                    .attr("mockClass", mock.getClass().getName())
                    .attr("mock", mock)
                    .log("Mock contains registered invocations that should be cleared");
            invocationContainer.clearInvocations();
        }
    }

    public boolean isEnabled() {
        return MOCKING_PROGRESS_PROVIDER != null;
    }
}
