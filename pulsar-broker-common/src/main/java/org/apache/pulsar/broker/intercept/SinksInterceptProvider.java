package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.io.SinkConfig;

public interface SinksInterceptProvider {
    /**
     * Intercept call for create sink
     *
     * @param sinkConfig the sink config of the sink to be created
     * @param clientRole the role used to create sink
     */
    default void createSink(SinkConfig sinkConfig, String clientRole) throws InterceptException {} ;

    /**
     * Intercept call for update sink
     *  @param sinkConfig the sink config of the sink to be updated
     * @param existingSinkConfig
     * @param clientRole the role used to update sink
     */
    default void updateSink(SinkConfig sinkConfig, SinkConfig existingSinkConfig, String clientRole) throws InterceptException {}
}
