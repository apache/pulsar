package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.common.policies.data.TenantInfo;

public class TenantsInterceptService {

    private final TenantsInterceptProvider provider;

    public TenantsInterceptService(TenantsInterceptProvider tenantInterceptProvider) {
        this.provider = tenantInterceptProvider;
    }

    /**
     * Intercept call for create tenant
     *
     * @param tenant tenant name
     * @param tenantInfo tenant info
     * @param clientRole the role used to create tenant
     */
    public void createTenant(String tenant, TenantInfo tenantInfo, String clientRole) throws InterceptException {
        provider.createTenant(tenant, tenantInfo, clientRole);
    }
}
