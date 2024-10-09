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
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.util.HostPort;
import org.eclipse.jetty.util.component.ContainerLifeCycle;

/**
 * Class to standardize initialization of a Jetty request logger for all pulsar components.
 */
public class JettyRequestLogFactory {

    /**
     * The time format to use for request logging. This custom format is necessary because the
     * default option uses GMT for the time zone. Pulsar's request logging has historically
     * used the JVM's default time zone, so this format uses that time zone. It is also necessary
     * because the {@link CustomRequestLog#DEFAULT_DATE_FORMAT} is "dd/MMM/yyyy:HH:mm:ss ZZZ" instead
     * of "dd/MMM/yyyy:HH:mm:ss Z" (the old date format). The key difference is that ZZZ will render
     * the strict offset for the timezone that is unaware of daylight savings time while the Z will
     * render the offset based on daylight savings time.
     *
     * As the javadoc for {@link CustomRequestLog} describes, the time code can take two arguments to
     * configure the format and the time zone. They must be in the form: "%{format|timeZone}t".
     */
    private static final String TIME_FORMAT = String.format(" %%{%s|%s}t ",
            "dd/MMM/yyyy:HH:mm:ss Z",
            TimeZone.getDefault().getID());

    /**
     * This format is essentially the {@link CustomRequestLog#EXTENDED_NCSA_FORMAT} with three modifications:
     *   1. The time zone will be the JVM's default time zone instead of always being GMT.
     *   2. The time zone offset will be daylight savings time aware.
     *   3. The final value will be the request time (latency) in milliseconds.
     *
     * See javadoc for {@link CustomRequestLog} for more information.
     */
    private static final String LOG_FORMAT =
            "%{client}a - %u" + TIME_FORMAT + "\"%r\" %s %O \"%{Referer}i\" \"%{User-Agent}i\" %{ms}T";

    /**
     * Build a new Jetty request logger using the format defined in this class.
     * @return a request logger
     */
    public static RequestLog createRequestLogger() {
        return createRequestLogger(false, null);
    }

    /**
     * Build a new Jetty request logger using the format defined in this class.
     * @param showDetailedAddresses whether to show detailed addresses and ports in logs
     * @return a request logger
     */
    public static RequestLog createRequestLogger(boolean showDetailedAddresses, Server server) {
        if (!showDetailedAddresses) {
            return new CustomRequestLog(new Slf4jRequestLogWriter(), LOG_FORMAT);
        } else {
            return new OriginalClientIPRequestLog(server);
        }
    }

    /**
     * Logs the original and real remote (client) and local (server) IP addresses
     * when detailed addresses are enabled.
     * Tracks the real addresses of remote and local using a registered Connection.Listener
     * when detailed addresses are enabled.
     * This is necessary when Proxy Protocol is used to pass the original client IP.
     */
    @Slf4j
    private static class OriginalClientIPRequestLog extends ContainerLifeCycle implements RequestLog {
        private final ThreadLocal<StringBuilder> requestLogStringBuilder = ThreadLocal.withInitial(StringBuilder::new);
        private final CustomRequestLog delegate;
        private final Slf4jRequestLogWriter delegateLogWriter;

        OriginalClientIPRequestLog(Server server) {
            delegate = new CustomRequestLog(this::write, LOG_FORMAT);
            addBean(delegate);
            delegateLogWriter = new Slf4jRequestLogWriter();
            addBean(delegateLogWriter);
            if (server != null) {
                for (Connector connector : server.getConnectors()) {
                    // adding the listener is only necessary for connectors that use ProxyConnectionFactory
                    if (connector.getDefaultConnectionFactory() instanceof ProxyConnectionFactory) {
                        connector.addBean(proxyProtocolOriginalEndpointListener);
                    }
                }
            }
        }

        void write(String requestEntry) {
            StringBuilder sb = requestLogStringBuilder.get();
            sb.setLength(0);
            sb.append(requestEntry);
        }

        @Override
        public void log(Request request, Response response) {
            delegate.log(request, response);
            StringBuilder sb = requestLogStringBuilder.get();
            sb.append(" [R:");
            sb.append(request.getRemoteHost());
            sb.append(':');
            sb.append(request.getRemotePort());
            InetSocketAddress realRemoteAddress = lookupRealAddress(request.getHttpChannel().getRemoteAddress());
            if (realRemoteAddress != null) {
                String realRemoteHost = HostPort.normalizeHost(realRemoteAddress.getHostString());
                int realRemotePort = realRemoteAddress.getPort();
                if (!realRemoteHost.equals(request.getRemoteHost()) || realRemotePort != request.getRemotePort()) {
                    sb.append(" via ");
                    sb.append(realRemoteHost);
                    sb.append(':');
                    sb.append(realRemotePort);
                }
            }
            sb.append("]->[L:");
            InetSocketAddress realLocalAddress = lookupRealAddress(request.getHttpChannel().getLocalAddress());
            if (realLocalAddress != null) {
                String realLocalHost = HostPort.normalizeHost(realLocalAddress.getHostString());
                int realLocalPort = realLocalAddress.getPort();
                sb.append(realLocalHost);
                sb.append(':');
                sb.append(realLocalPort);
                if (!realLocalHost.equals(request.getLocalAddr()) || realLocalPort != request.getLocalPort()) {
                    sb.append(" dst ");
                    sb.append(request.getLocalAddr());
                    sb.append(':');
                    sb.append(request.getLocalPort());
                }
            } else {
                sb.append(request.getLocalAddr());
                sb.append(':');
                sb.append(request.getLocalPort());
            }
            sb.append(']');
            try {
                delegateLogWriter.write(sb.toString());
            } catch (Exception e) {
                log.warn("Failed to write request log", e);
            }
        }

        private InetSocketAddress lookupRealAddress(InetSocketAddress socketAddress) {
            if (socketAddress == null) {
                return null;
            }
            if (proxyProtocolRealAddressMapping.isEmpty()) {
                return socketAddress;
            }
            AddressEntry entry = proxyProtocolRealAddressMapping.get(new AddressKey(socketAddress.getHostString(),
                    socketAddress.getPort()));
            if (entry != null) {
                return entry.realAddress;
            } else {
                return socketAddress;
            }
        }

        private final Connection.Listener proxyProtocolOriginalEndpointListener =
                new ProxyProtocolOriginalEndpointListener();

        private final ConcurrentHashMap<AddressKey, AddressEntry> proxyProtocolRealAddressMapping =
                new ConcurrentHashMap<>();

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
                    mapAddress(proxyEndPoint.getLocalAddress(), originalEndpoint.getLocalAddress(), increment);
                    mapAddress(proxyEndPoint.getRemoteAddress(), originalEndpoint.getRemoteAddress(), increment);
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
}
