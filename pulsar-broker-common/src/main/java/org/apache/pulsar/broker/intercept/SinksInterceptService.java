package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.io.SinkConfig;

public class SinksInterceptService {

    private final SinksInterceptProvider provider;

    public SinksInterceptService(SinksInterceptProvider sinksInterceptProvider) {
        this.provider = sinksInterceptProvider;
    }

    /**
     * Intercept call for create sink
     *
     * @param sinkConfig the sink config of the sink to be created
     * @param clientRole the role used to create sink
     */
    public void createSink(SinkConfig sinkConfig, String clientRole) throws InterceptException {
        provider.createSink(sinkConfig, clientRole);
    }

    /**
     * Intercept call for update sink
     *  @param updates updates to this sink's source config
     * @param existingSinkConfig the existing source config
     * @param clientRole the role used to update sink
     */
    public void updateSink(SinkConfig updates, SinkConfig existingSinkConfig, String clientRole) throws InterceptException {
        provider.updateSink(updates, existingSinkConfig, clientRole);
    }
}
