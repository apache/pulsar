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
package org.apache.pulsar.common.util.netty;

import static org.testng.Assert.assertEquals;
import com.google.common.net.InetAddresses;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.NameResolver;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class DnsResolverUtilTest {

    @Test
    public void testNameResolverAdapter() throws ExecutionException, InterruptedException {
        ImmediateEventExecutor executor = ImmediateEventExecutor.INSTANCE;
        AddressResolver<InetSocketAddress> addressResolver =
                new DefaultNameResolver(executor).asAddressResolver();
        NameResolver<InetAddress> nameResolverAdapter =
                DnsResolverUtil.createNameResolverAdapter(addressResolver, executor);
        testNameResolver(nameResolverAdapter);
    }

    @Test
    public void testNameResolverExtraction() throws ExecutionException, InterruptedException {
        ImmediateEventExecutor executor = ImmediateEventExecutor.INSTANCE;
        AddressResolver<InetSocketAddress> addressResolver =
                new DefaultNameResolver(executor).asAddressResolver();
        NameResolver<InetAddress> nameResolverAdapter =
                DnsResolverUtil.adaptToNameResolver(addressResolver);
        testNameResolver(nameResolverAdapter);
    }

    @Test
    public void testNameResolverNullFallback() throws ExecutionException, InterruptedException {
        NameResolver<InetAddress> nameResolverAdapter = DnsResolverUtil.adaptToNameResolver(null);
        testNameResolver(nameResolverAdapter);
    }

    private static void testNameResolver(NameResolver<InetAddress> nameResolver)
            throws InterruptedException, ExecutionException {
        InetAddress inetAddress = nameResolver.resolve("8.8.8.8").get();
        assertEquals(inetAddress, InetAddresses.forString("8.8.8.8"));
        List<InetAddress> inetAddresses = nameResolver.resolveAll("8.8.8.8").get();
        assertEquals(inetAddresses.get(0), InetAddresses.forString("8.8.8.8"));
    }
}