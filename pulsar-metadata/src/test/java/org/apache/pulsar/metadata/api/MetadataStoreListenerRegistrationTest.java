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
package org.apache.pulsar.metadata.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.function.Consumer;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.testng.annotations.Test;

/**
 * The default listener registration methods, which keep the implementations written before the cancellable methods
 * working, and route the callers of the deprecated methods to the cancellable ones.
 */
public class MetadataStoreListenerRegistrationTest {

    @Test
    @SuppressWarnings("deprecation")
    public void testCancellableRegistrationFallsBackToDeprecatedMethods() {
        // An implementation that only implements the deprecated methods
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        doCallRealMethod().when(store).registerCancellableListener(any());
        doCallRealMethod().when(store).registerCancellableSessionListener(any());
        Consumer<Notification> listener = notification -> { };
        Consumer<SessionEvent> sessionListener = event -> { };

        Runnable cancelListener = store.registerCancellableListener(listener);
        Runnable cancelSessionListener = store.registerCancellableSessionListener(sessionListener);

        verify(store).registerListener(listener);
        verify(store).registerSessionListener(sessionListener);
        // The callbacks can't unregister the listeners, but they can be run
        assertThat(cancelListener).isNotNull();
        assertThat(cancelSessionListener).isNotNull();
        cancelListener.run();
        cancelSessionListener.run();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedRegistrationUsesCancellableMethods() {
        // An implementation that implements the cancellable methods
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        doCallRealMethod().when(store).registerListener(any());
        doCallRealMethod().when(store).registerSessionListener(any());
        when(store.registerCancellableListener(any())).thenReturn(() -> { });
        when(store.registerCancellableSessionListener(any())).thenReturn(() -> { });
        Consumer<Notification> listener = notification -> { };
        Consumer<SessionEvent> sessionListener = event -> { };

        store.registerListener(listener);
        store.registerSessionListener(sessionListener);

        verify(store).registerCancellableListener(listener);
        verify(store).registerCancellableSessionListener(sessionListener);
    }
}
