package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.io.SourceConfig;

public interface SourcesInterceptProvider {
    /**
     * Intercept call for create source
     *
     * @param sourceConfig the source config of the source to be created
     * @param clientRole the role used to create source
     */
    default void createSource(SourceConfig sourceConfig, String clientRole) throws InterceptException {}

    /**
     * Intercept call for update source
     *  @param sourceConfig the source config of the source to be updated
     * @param existingSourceConfig
     * @param clientRole the role used to update source
     */
    default void updateSource(SourceConfig sourceConfig, SourceConfig existingSourceConfig, String clientRole) throws InterceptException {}
}
