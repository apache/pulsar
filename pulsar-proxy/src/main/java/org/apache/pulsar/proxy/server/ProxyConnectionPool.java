package org.apache.pulsar.proxy.server;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;

public class ProxyConnectionPool extends ConnectionPool {
    public ProxyConnectionPool(ClientConfigurationData clientConfig, EventLoopGroup eventLoopGroup,
            Supplier<ClientCnx> clientCnxSupplier) {
        super(clientConfig, eventLoopGroup, clientCnxSupplier);
    }

    @Override
    public void close() throws IOException {
        log.info("Closing ProxyConnectionPool.");
        pool.forEach((address, clientCnxPool) -> {
            if (clientCnxPool != null) {
                clientCnxPool.forEach((identifier, clientCnx) -> {
                    if (clientCnx != null && clientCnx.isDone()) {
                        try {
                            clientCnx.get().close();
                        } catch (InterruptedException | ExecutionException e) {
                            log.error("Unable to close get client connection future.", e);
                        }
                    }
                });
            }
        });
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyConnectionPool.class);
}