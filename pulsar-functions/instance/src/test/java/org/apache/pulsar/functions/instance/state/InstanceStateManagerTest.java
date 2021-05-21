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
package org.apache.pulsar.functions.instance.state;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.apache.pulsar.functions.api.StateStore;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link InstanceStateManager}.
 */
public class InstanceStateManagerTest {

    private InstanceStateManager stateManager;

    @BeforeMethod
    public void setup() {
        this.stateManager = new InstanceStateManager();
    }

    @Test
    public void testGetStoreNull() {
        final String fqsn = "t/ns/store";
        StateStore getStore = stateManager.getStore("t", "ns", "store");
        assertNull(getStore);
    }

    @Test
    public void testRegisterStore() {
        final String fqsn = "t/ns/store";
        StateStore store = mock(StateStore.class);
        when(store.fqsn()).thenReturn(fqsn);
        this.stateManager.registerStore(store);
        StateStore getStore = stateManager.getStore("t", "ns", "store");
        assertSame(getStore, store);
    }

    @Test
    public void testRegisterStoreTwice() {
        final String fqsn = "t/ns/store";
        StateStore store = mock(StateStore.class);
        when(store.fqsn()).thenReturn(fqsn);
        this.stateManager.registerStore(store);
        try {
            this.stateManager.registerStore(store);
            fail("Should fail to register a store twice");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testClose() {
        final String fqsn1 = "t/ns/store-1";
        StateStore store1 = mock(StateStore.class);
        when(store1.fqsn()).thenReturn(fqsn1);
        final String fqsn2 = "t/ns/store-2";
        StateStore store2 = mock(StateStore.class);
        when(store2.fqsn()).thenReturn(fqsn2);

        this.stateManager.registerStore(store1);
        this.stateManager.registerStore(store2);

        this.stateManager.close();

        verify(store1, times(1)).close();
        verify(store2, times(1)).close();
    }

    @Test
    public void testCloseException() {
        final String fqsn1 = "t/ns/store-1";
        StateStore store1 = mock(StateStore.class);
        when(store1.fqsn()).thenReturn(fqsn1);
        RuntimeException exception1 = new RuntimeException("exception 1");
        doThrow(exception1).when(store1).close();
        final String fqsn2 = "t/ns/store-2";
        StateStore store2 = mock(StateStore.class);
        when(store2.fqsn()).thenReturn(fqsn2);
        RuntimeException exception2 = new RuntimeException("exception 2");
        doThrow(exception2).when(store2).close();

        this.stateManager.registerStore(store2);
        this.stateManager.registerStore(store1);

        try {
            this.stateManager.close();
            fail("Should fail to close the state manager");
        } catch (RuntimeException re) {
            assertSame(re, exception2);
        }

        assertTrue(this.stateManager.isEmpty());
    }

}
