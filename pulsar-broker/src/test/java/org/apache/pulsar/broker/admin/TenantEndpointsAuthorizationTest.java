package org.apache.pulsar.broker.admin;

import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@Test(groups = "broker-admin")
public class TenantEndpointsAuthorizationTest extends MockedPulsarStandalone {

    private AuthorizationService orignalAuthorizationService;
    private AuthorizationService spyAuthorizationService;

    private PulsarAdmin superUserAdmin;
    private PulsarAdmin nobodyAdmin;

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    public void setup() {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        start();
        this.superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        this.nobodyAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(NOBODY_TOKEN))
                .build();
    }

    @BeforeMethod(alwaysRun = true)
    public void before() throws IllegalAccessException {
        orignalAuthorizationService = getPulsarService().getBrokerService().getAuthorizationService();
        spyAuthorizationService = spy(orignalAuthorizationService);
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                spyAuthorizationService, true);
    }

    @AfterMethod(alwaysRun = true)
    public void after() throws IllegalAccessException {
        if (orignalAuthorizationService != null) {
            FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService", orignalAuthorizationService, true);
        }
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        if (superUserAdmin != null) {
            superUserAdmin.close();
            superUserAdmin = null;
        }
        spyAuthorizationService = null;
        orignalAuthorizationService = null;
        super.close();
    }

    @Test
    public void testListTenants() throws PulsarAdminException {
        superUserAdmin.tenants().getTenants();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowTenantOperationAsync(isNull(), Mockito.eq(TenantOperation.LIST_TENANTS), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.tenants().getTenants());
    }


    @Test
    public void testGetTenant() throws PulsarAdminException {
        String tenantName = "public";
        superUserAdmin.tenants().getTenantInfo(tenantName);
        final String brokerId = getPulsarService().getBrokerId();
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_BROKERS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getActiveBrokers());
    }

    @Test
    public void testUpdateTenant() throws PulsarAdminException {
        superUserAdmin.brokers().getActiveBrokers();
        final String brokerId = getPulsarService().getBrokerId();
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_BROKERS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getActiveBrokers());
    }

    @Test
    public void testDeleteTenant() throws PulsarAdminException {
        superUserAdmin.brokers().getActiveBrokers();
        final String brokerId = getPulsarService().getBrokerId();
        final String clusterName = getPulsarService().getConfiguration().getClusterName();
        // test allow broker operation
        verify(spyAuthorizationService)
                .allowBrokerOperationAsync(eq(clusterName), eq(brokerId), eq(BrokerOperation.LIST_BROKERS), any(), any(), any());
        // fallback to superuser
        verify(spyAuthorizationService).isSuperUser(any(), any());

        // ---- test nobody
        Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> nobodyAdmin.brokers().getActiveBrokers());
    }
}
