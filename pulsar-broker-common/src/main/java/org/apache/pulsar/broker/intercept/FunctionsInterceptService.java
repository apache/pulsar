package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.functions.FunctionConfig;

public class FunctionsInterceptService {

    private final FunctionsInterceptProvider provider;

    public FunctionsInterceptService(FunctionsInterceptProvider functionsInterceptProvider) {
        this.provider = functionsInterceptProvider;
    }

    /**
     * Intercept call for create function
     *
     * @param functionConfig function config of the function to be created
     * @param clientRole the role used to create function
     */
    public void createFunction(FunctionConfig functionConfig, String clientRole) throws InterceptException {
        provider.createFunction(functionConfig, clientRole);
    }

    /**
     * Intercept call for update source
     *
     * @param updates updates to this function's function config
     * @param existingFunctionConfig the existing function config
     * @param clientRole the role used to update function
     */
    public void updateFunction(FunctionConfig updates, FunctionConfig existingFunctionConfig, String clientRole) throws InterceptException {
        provider.updateFunction(updates, existingFunctionConfig, clientRole);
    }

}
