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

package org.apache.pulsar.proxy.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import io.netty.resolver.AddressResolver;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SucceededFuture;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import org.apache.curator.shaded.com.google.common.net.InetAddresses;
import org.testng.annotations.Test;

public class BrokerProxyValidatorTest {

    @Test
    public void shouldAllowValidInput() throws Exception {
        BrokerProxyValidator brokerProxyValidator = new BrokerProxyValidator(
                createMockedAddressResolver("1.2.3.4"),
                "myhost"
                , "1.2.0.0/16"
                , "6650");
        InetSocketAddress inetSocketAddress = brokerProxyValidator.resolveAndCheckTargetAddress("myhost:6650").get();
        assertNotNull(inetSocketAddress);
        assertEquals(inetSocketAddress.getAddress().getHostAddress(), "1.2.3.4");
        assertEquals(inetSocketAddress.getPort(), 6650);
    }

    @Test(expectedExceptions = ExecutionException.class,
            expectedExceptionsMessageRegExp = ".*Given host in 'myhost:6650' isn't allowed.")
    public void shouldPreventInvalidHostName() throws Exception {
        BrokerProxyValidator brokerProxyValidator = new BrokerProxyValidator(
                createMockedAddressResolver("1.2.3.4"),
                "allowedhost"
                , "1.2.0.0/16"
                , "6650");
        brokerProxyValidator.resolveAndCheckTargetAddress("myhost:6650").get();
    }

    @Test(expectedExceptions = ExecutionException.class,
            expectedExceptionsMessageRegExp = ".* The IP address of the given host and port 'myhost:6650' isn't allowed.")
    public void shouldPreventInvalidIPAddress() throws Exception {
        BrokerProxyValidator brokerProxyValidator = new BrokerProxyValidator(
                createMockedAddressResolver("1.2.3.4"),
                "myhost"
                , "1.3.0.0/16"
                , "6650");
        brokerProxyValidator.resolveAndCheckTargetAddress("myhost:6650").get();
    }

    @Test
    public void shouldSupportHostNamePattern() throws Exception {
        BrokerProxyValidator brokerProxyValidator = new BrokerProxyValidator(
                createMockedAddressResolver("1.2.3.4"),
                "*.mydomain"
                , "1.2.0.0/16"
                , "6650");
        brokerProxyValidator.resolveAndCheckTargetAddress("myhost.mydomain:6650").get();
    }

    @Test
    public void shouldAllowAllWithWildcard() throws Exception {
        BrokerProxyValidator brokerProxyValidator = new BrokerProxyValidator(
                createMockedAddressResolver("1.2.3.4"),
                "*"
                , "*"
                , "6650");
        brokerProxyValidator.resolveAndCheckTargetAddress("myhost.mydomain:6650").get();
    }

    @Test
    public void shouldAllowIPv6Address() throws Exception {
        BrokerProxyValidator brokerProxyValidator = new BrokerProxyValidator(
                createMockedAddressResolver("fd4d:801b:73fa:abcd:0000:0000:0000:0001"),
                "*"
                , "fd4d:801b:73fa:abcd::/64"
                , "6650");
        brokerProxyValidator.resolveAndCheckTargetAddress("myhost.mydomain:6650").get();
    }

    @Test
    public void shouldAllowIPv6AddressNumeric() throws Exception {
        BrokerProxyValidator brokerProxyValidator = new BrokerProxyValidator(
                createMockedAddressResolver("fd4d:801b:73fa:abcd:0000:0000:0000:0001"),
                "*"
                , "fd4d:801b:73fa:abcd::/64"
                , "6650");
        brokerProxyValidator.resolveAndCheckTargetAddress("fd4d:801b:73fa:abcd:0000:0000:0000:0001:6650").get();
    }

    private AddressResolver<InetSocketAddress> createMockedAddressResolver(String ipAddressResult) {
        AddressResolver<InetSocketAddress> inetSocketAddressResolver = mock(AddressResolver.class);
        when(inetSocketAddressResolver.resolve(any())).then(invocationOnMock -> {
            InetSocketAddress address = (InetSocketAddress) invocationOnMock.getArgument(0);
            return new SucceededFuture<SocketAddress>(mock(EventExecutor.class),
                    new InetSocketAddress(InetAddresses.forString(ipAddressResult), address.getPort()));
        });
        return inetSocketAddressResolver;
    }
}
