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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.net.InetAddress;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@Slf4j
public class AuthenticationInitContextImplTest {
    @Test
    public void testConstructorWithSharedResources() {
        EventLoopGroup eventLoopGroup = Mockito.mock(EventLoopGroup.class);
        Timer timer = Mockito.mock(Timer.class);
        NameResolver<InetAddress> nameResolver = Mockito.mock(NameResolver.class);

        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(
                eventLoopGroup, timer, nameResolver);

        Optional<EventLoopGroup> retrievedEventLoop = context.getService(EventLoopGroup.class);
        Optional<Timer> retrievedTimer = context.getService(Timer.class);
        Optional<NameResolver> retrievedNameResolver = context.getService(NameResolver.class);

        assertTrue(retrievedEventLoop.isPresent());
        assertTrue(retrievedTimer.isPresent());
        assertTrue(retrievedNameResolver.isPresent());
        assertEquals(retrievedEventLoop.get(), eventLoopGroup);
        assertEquals(retrievedTimer.get(), timer);
        assertEquals(retrievedNameResolver.get(), nameResolver);
    }

    @Test
    public void testConstructorWithNullResources() {
        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(
                null, null, null);

        Optional<EventLoopGroup> eventLoop = context.getService(EventLoopGroup.class);
        Optional<Timer> timer = context.getService(Timer.class);
        Optional<NameResolver> nameResolver = context.getService(NameResolver.class);

        assertFalse(eventLoop.isPresent());
        assertFalse(timer.isPresent());
        assertFalse(nameResolver.isPresent());
    }

    @Test
    public void testAddAndGetService() {
        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(null, null, null);
        String testService = "Test Service";

        context.addService(String.class, testService);

        Optional<String> retrieved = context.getService(String.class);
        assertTrue(retrieved.isPresent());
        assertEquals(retrieved.get(), testService);
    }

    @Test
    public void testAddAndGetNamedService() {
        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(null, null, null);
        String namedService1 = "Named Service 1";
        String namedService2 = "Named Service 2";

        context.addService(String.class, "name1", namedService1);
        context.addService(String.class, "name2", namedService2);

        Optional<String> retrieved1 = context.getServiceByName(String.class, "name1");
        Optional<String> retrieved2 = context.getServiceByName(String.class, "name2");
        Optional<String> notFound = context.getServiceByName(String.class, "nonexistent");

        assertTrue(retrieved1.isPresent());
        assertEquals(retrieved1.get(), namedService1);
        assertTrue(retrieved2.isPresent());
        assertEquals(retrieved2.get(), namedService2);
        assertFalse(notFound.isPresent());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetServiceWithNullClass() {
        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(null, null, null);

        context.getService(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetServiceByNameWithNullName() {
        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(null, null, null);

        context.getServiceByName(String.class, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetServiceByNameWithEmptyName() {
        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(null, null, null);

        context.getServiceByName(String.class, "");
    }

    @Test
    public void testNamedServicesAreIndependent() {
        AuthenticationInitContextImpl context = new AuthenticationInitContextImpl(null, null, null);
        Object service1 = new Object();
        Object service2 = new Object();

        context.addService(Object.class, "group1", service1);
        context.addService(Object.class, "group2", service2);

        Optional<Object> fromGroup1 = context.getServiceByName(Object.class, "group1");
        Optional<Object> fromGroup2 = context.getServiceByName(Object.class, "group2");

        assertTrue(fromGroup1.isPresent());
        assertTrue(fromGroup2.isPresent());
        assertNotSame(fromGroup1.get(), fromGroup2.get());
    }
}
