package org.apache.flink.streaming.connectors.pulsar;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class CachedPulsarClient {
    private static final Logger LOG = LoggerFactory.getLogger(CachedPulsarClient.class);

    private static int cacheSize = 5;

    public static void setCacheSize(int size) {
        cacheSize = size;
    }

    private static CacheLoader<ClientConfigurationData, PulsarClientImpl> cacheLoader = new CacheLoader<ClientConfigurationData, PulsarClientImpl>() {
        @Override
        public PulsarClientImpl load(ClientConfigurationData key) throws Exception {
            return createPulsarClient(key);
        }
    };

    private static RemovalListener<ClientConfigurationData, PulsarClientImpl> removalListener = notification -> {
        ClientConfigurationData config = notification.getKey();
        PulsarClientImpl client = notification.getValue();
        LOG.debug("Evicting pulsar client %s with config %s, due to %s", client.toString(), config.toString(), notification.getCause().toString());
        close(config, client);
    };

    private static LoadingCache<ClientConfigurationData, PulsarClientImpl> guavaCache =
        CacheBuilder.newBuilder().maximumSize(cacheSize).removalListener(removalListener).build(cacheLoader);

    private static PulsarClientImpl createPulsarClient(ClientConfigurationData clientConfig) throws PulsarClientException {
        PulsarClientImpl client;
        try {
            client = new PulsarClientImpl(clientConfig);
            LOG.debug(String.format("Created a new instance of PulsarClientImpl for clientConf = %s", clientConfig.toString()));
        } catch (PulsarClientException e) {
            LOG.error(String.format("Failed to create PulsarClientImpl for clientConf = %s", clientConfig.toString()));
            throw e;
        }
        return client;
    }

    public static PulsarClientImpl getOrCreate(ClientConfigurationData config) throws ExecutionException {
        return guavaCache.get(config);
    }

    private static void close(ClientConfigurationData clientConfig, PulsarClientImpl client) {
        try {
            LOG.info(String.format("Closing the Pulsar client with conifg %s", clientConfig.toString()));
            client.close();
        } catch (PulsarClientException e) {
            LOG.warn(String.format("Error while closing the Pulsar client ", clientConfig.toString()), e);
        }
    }

    private static void clear() {
        LOG.info("Cleaning up guava cache.");
        guavaCache.invalidateAll();
    }
}
