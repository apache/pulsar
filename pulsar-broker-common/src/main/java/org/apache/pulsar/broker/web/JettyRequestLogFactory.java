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
package org.apache.pulsar.broker.web;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Principal;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.security.AuthenticationState;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Attributes;
import org.eclipse.jetty.util.HostPort;
import org.eclipse.jetty.util.NanoTime;
import org.eclipse.jetty.util.component.ContainerLifeCycle;

/**
 * Class to standardize initialization of a Jetty request logger for all pulsar components.
 *
 * <p>Emits one structured log entry per HTTP request with individual fields as slog attributes
 * (method, uri, status, clientAddr, bytesOut, durationMs, etc.) instead of the previous
 * pre-formatted NCSA combined log line.
 */
@CustomLog
public class JettyRequestLogFactory {

    /**
     * Build a new Jetty request logger.
     * @return a request logger
     */
    public static RequestLog createRequestLogger() {
        return createRequestLogger(false, null);
    }

    /**
     * Build a new Jetty request logger.
     * @param showDetailedAddresses whether to show detailed addresses and ports in logs
     * @return a request logger
     */
    public static RequestLog createRequestLogger(boolean showDetailedAddresses, Server server) {
        if (!showDetailedAddresses) {
            return new StructuredRequestLog();
        } else {
            return new OriginalClientIPRequestLog(server);
        }
    }

    /**
     * Structured request logger that emits one slog line per request with each field as an attribute.
     */
    private static class StructuredRequestLog implements RequestLog {
        @Override
        public void log(Request request, Response response) {
            logRequest(request, response, null);
        }
    }

    /**
     * Emits the same structured attributes as {@link StructuredRequestLog} and additionally records
     * the real client and server addresses when Proxy Protocol wraps the original endpoint.
     */
    private static class OriginalClientIPRequestLog extends ContainerLifeCycle implements RequestLog {
        private final Connection.Listener proxyProtocolOriginalEndpointListener =
                new ProxyProtocolOriginalEndpointListener();
        private final ConcurrentHashMap<AddressKey, AddressEntry> proxyProtocolRealAddressMapping =
                new ConcurrentHashMap<>();

        OriginalClientIPRequestLog(Server server) {
            if (server != null) {
                for (Connector connector : server.getConnectors()) {
                    // adding the listener is only necessary for connectors that use ProxyConnectionFactory
                    if (connector.getDefaultConnectionFactory() instanceof ProxyConnectionFactory) {
                        connector.addBean(proxyProtocolOriginalEndpointListener);
                    }
                }
            }
        }

        @Override
        public void log(Request request, Response response) {
            logRequest(request, response, this);
        }

        private ConnectionMetaData unwrap(ConnectionMetaData connectionMetaData) {
            if (connectionMetaData instanceof Attributes) {
                return (ConnectionMetaData) Attributes.unwrap((Attributes) connectionMetaData);
            }
            return connectionMetaData;
        }

        private InetSocketAddress lookupRealAddress(SocketAddress socketAddress) {
            if (socketAddress == null || !(socketAddress instanceof InetSocketAddress)) {
                return null;
            }
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            if (proxyProtocolRealAddressMapping.isEmpty()) {
                return inetSocketAddress;
            }
            AddressEntry entry = proxyProtocolRealAddressMapping.get(new AddressKey(inetSocketAddress.getHostString(),
                    inetSocketAddress.getPort()));
            if (entry != null) {
                return entry.realAddress;
            } else {
                return inetSocketAddress;
            }
        }

        // Use a record as key since InetSocketAddress hash code changes if the address gets resolved
        record AddressKey(String hostString, int port) {
        }

        record AddressEntry(InetSocketAddress realAddress, AtomicInteger referenceCount) {
        }

        // Tracks the real addresses of remote and local when detailed addresses are enabled.
        // This is necessary when Proxy Protocol is used to pass the original client IP.
        // The Proxy Protocol implementation in Jetty wraps the original endpoint with a ProxyEndPoint
        // and the real endpoint information isn't available in the request object.
        // This listener is added to all connectors to track the real addresses of the client and server.
        class ProxyProtocolOriginalEndpointListener implements Connection.Listener {
            @Override
            public void onOpened(Connection connection) {
                handleConnection(connection, true);
            }

            @Override
            public void onClosed(Connection connection) {
                handleConnection(connection, false);
            }

            private void handleConnection(Connection connection, boolean increment) {
                if (connection.getEndPoint() instanceof ProxyConnectionFactory.ProxyEndPoint) {
                    ProxyConnectionFactory.ProxyEndPoint proxyEndPoint =
                            (ProxyConnectionFactory.ProxyEndPoint) connection.getEndPoint();
                    EndPoint originalEndpoint = proxyEndPoint.unwrap();
                    mapAddress((InetSocketAddress) proxyEndPoint.getLocalSocketAddress(),
                            (InetSocketAddress) originalEndpoint.getLocalSocketAddress(),
                            increment);
                    mapAddress((InetSocketAddress) proxyEndPoint.getRemoteSocketAddress(),
                            (InetSocketAddress) originalEndpoint.getRemoteSocketAddress(),
                            increment);
                }
            }

            private void mapAddress(InetSocketAddress current, InetSocketAddress real, boolean increment) {
                // don't add the mapping if the current address is the same as the real address
                if (real != null && current != null && current.equals(real)) {
                    return;
                }
                AddressKey key = new AddressKey(current.getHostString(), current.getPort());
                proxyProtocolRealAddressMapping.compute(key, (__, entry) -> {
                    if (entry == null) {
                        if (increment) {
                            entry = new AddressEntry(real, new AtomicInteger(1));
                        }
                    } else {
                        if (increment) {
                            entry.referenceCount.incrementAndGet();
                        } else {
                            if (entry.referenceCount.decrementAndGet() == 0) {
                                // remove the entry if the reference count drops to 0
                                entry = null;
                            }
                        }
                    }
                    return entry;
                });
            }
        }
    }

    private static void logRequest(Request request, Response response, OriginalClientIPRequestLog addressTracker) {
        log.info(e -> {
            e.attr("method", request.getMethod())
                    .attr("uri", request.getHttpURI().asString())
                    .attr("proto", request.getConnectionMetaData().getProtocol())
                    .attr("status", response.getStatus())
                    .attr("bytesOut", Response.getContentBytesWritten(response))
                    .attr("clientAddr", Request.getRemoteAddr(request))
                    .attr("clientPort", Request.getRemotePort(request))
                    .attr("user", authUser(request))
                    .attr("referer", request.getHeaders().get(HttpHeader.REFERER))
                    .attr("userAgent", request.getHeaders().get(HttpHeader.USER_AGENT))
                    .attr("durationMs", NanoTime.millisSince(request.getBeginNanoTime()));

            if (addressTracker != null) {
                String clientAddr = Request.getRemoteAddr(request);
                int clientPort = Request.getRemotePort(request);
                String localAddr = Request.getLocalAddr(request);
                int localPort = Request.getLocalPort(request);
                e.attr("localAddr", localAddr).attr("localPort", localPort);

                ConnectionMetaData md = addressTracker.unwrap(request.getConnectionMetaData());
                InetSocketAddress realRemote = addressTracker.lookupRealAddress(md.getRemoteSocketAddress());
                if (realRemote != null) {
                    String host = HostPort.normalizeHost(realRemote.getHostString());
                    int port = realRemote.getPort();
                    if (!host.equals(clientAddr) || port != clientPort) {
                        e.attr("clientAddrReal", host).attr("clientPortReal", port);
                    }
                }
                InetSocketAddress realLocal = addressTracker.lookupRealAddress(md.getLocalSocketAddress());
                if (realLocal != null) {
                    String host = HostPort.normalizeHost(realLocal.getHostString());
                    int port = realLocal.getPort();
                    if (!host.equals(localAddr) || port != localPort) {
                        e.attr("localAddrReal", host).attr("localPortReal", port);
                    }
                }
            }

            e.log("HTTP request");
        });
    }

    private static String authUser(Request request) {
        Principal principal = AuthenticationState.getUserPrincipal(request);
        return principal != null ? principal.getName() : null;
    }
}
