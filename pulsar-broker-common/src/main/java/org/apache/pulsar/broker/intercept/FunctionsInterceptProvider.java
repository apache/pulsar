package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;

public interface FunctionsInterceptProvider {
    /**
     * Intercept call for create function
     *
     * @param functionConfig function config of the function to be created
     * @param clientRole the role used to create function
     */
    default void createFunction(FunctionConfig functionConfig, String clientRole) throws InterceptException {}

    /**
     * Intercept call for update function
     *  @param functionConfig function config of the function to be updated
     * @param existingFunctionConfig
     * @param clientRole the role used to update function
     */
    default void updateFunction(FunctionConfig functionConfig, FunctionConfig existingFunctionConfig, String clientRole) throws InterceptException {}
}
