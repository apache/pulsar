package org.apache.pulsar.security.authz;

import io.jsonwebtoken.Jwts;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.security.MockedPulsarStandalone;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public final class DefaultAuthZWithPublicAPITest extends MockedPulsarStandalone {

    private static final String USER1_SUBJECT =  "user1";
    private static final String USER1_TOKEN = Jwts.builder()
            .claim("sub", USER1_SUBJECT).signWith(SECRET_KEY).compact();

    private PulsarAdmin user1Admin;

    private PulsarAdmin superUserAdmin;
    @SneakyThrows
    @BeforeClass
    public void before() {
        loadTokenAuthentication();
        loadDefaultAuthorization();
        start();
        this.user1Admin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(USER1_TOKEN))
                .build();
        this.superUserAdmin =PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
    }


    @SneakyThrows
    @AfterClass
    public void after() {
        close();
    }



    @SneakyThrows
    @Test
    public void testConsumeWithTopicPolicyRetention() {
        final String random = UUID.randomUUID().toString();
        final String topic = "persistent://public/default/" + random;

        // grant consume permission to user 1, it can lookup and consume messages
        superUserAdmin.namespaces().grantPermissionOnNamespace("public/default",
                USER1_SUBJECT, Set.of(AuthAction.consume));
        superUserAdmin.topics().createNonPartitionedTopic(topic);

        // the user 1 shouldn't touch retention policy
        try {
            user1Admin.topicPolicies().getRetention(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            final RetentionPolicies policies = new RetentionPolicies(1, 1);
            user1Admin.topicPolicies().setRetention(topic, policies);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }

        try {
            user1Admin.topicPolicies().removeRetention(topic);
            Assert.fail("unexpected behaviour");
        } catch (PulsarAdminException ex) {
            Assert.assertTrue(ex instanceof PulsarAdminException.NotAuthorizedException);
        }
    }
}
