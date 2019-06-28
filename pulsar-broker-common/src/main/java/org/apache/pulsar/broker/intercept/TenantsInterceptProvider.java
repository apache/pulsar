package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.policies.data.TenantInfo;

public interface TenantsInterceptProvider {
    /**
     * Intercept call for create tenant
     *
     * @param tenant tenant name
     * @param tenantInfo tenant info
     * @param clientRole the role used to create tenant
     */
    default void createTenant(String tenant, TenantInfo tenantInfo, String clientRole) throws InterceptException {}
}
