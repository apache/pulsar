package org.apache.pulsar.broker.admin;

import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.testng.annotations.*;

import static org.mockito.Mockito.spy;

@Test(groups = "broker-admin")
public class ClusterEndpointsAuthorizationTest extends MockedPulsarStandalone {

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


}
