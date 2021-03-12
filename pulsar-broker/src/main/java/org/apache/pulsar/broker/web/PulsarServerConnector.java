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
package org.apache.pulsar.broker.web;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class PulsarServerConnector extends ServerConnector {

    // Throttle down the accept rate to limit the number of active TCP connections
    private final Semaphore semaphore = new Semaphore(10000);

    /**
     * @param server
     * @param acceptors
     * @param selectors
     */
    public PulsarServerConnector(Server server, int acceptors, int selectors) {
        super(server, acceptors, selectors);
    }

    /**
     * @param server
     * @param acceptors
     * @param selectors
     * @param sslContextFactory
     */
    public PulsarServerConnector(Server server, int acceptors, int selectors, SslContextFactory sslContextFactory) {
        super(server, acceptors, selectors, sslContextFactory);
    }

    @Override
    public void accept(int acceptorID) throws IOException {
        try {
            semaphore.acquire();
            super.accept(acceptorID);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void onEndPointClosed(EndPoint endp) {
        semaphore.release();
        super.onEndPointClosed(endp);
    }
}
