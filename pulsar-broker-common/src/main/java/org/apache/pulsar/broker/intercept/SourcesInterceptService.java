package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.io.SourceConfig;

public class SourcesInterceptService {

    private SourcesInterceptProvider provider;

    public SourcesInterceptService(SourcesInterceptProvider sourcesInterceptProvider) {
        this.provider = sourcesInterceptProvider;
    }

    /**
     * Intercept call for create source
     *
     * @param sourceConfig the source config of the source to be created
     * @param clientRole the role used to create source
     */
    public void createSource(SourceConfig sourceConfig, String clientRole) throws InterceptException {
        provider.createSource(sourceConfig, clientRole);
    }

    /**
     * Intercept call for update source
     *  @param updates updates to this source's source config
     * @param existingSourceConfig the existing source config
     * @param clientRole the role used to update source
     */
    public void updateSource(SourceConfig updates, SourceConfig existingSourceConfig, String clientRole) throws InterceptException {
        provider.updateSource(updates, existingSourceConfig, clientRole);
    }
}
