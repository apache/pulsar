package org.apache.pulsar.transaction.configuration;

import lombok.Getter;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

import java.util.Properties;

@Getter
public class CoordinatorConfiguration implements PulsarConfiguration {

    @Category
    private static final String CATEGORY_COORDINATOR = "Coordinator Settings";
    @Category
    private static final String CATEGORY_SECURITY = "Security settings for talking to brokers";

    @FieldContext(
            category = CATEGORY_COORDINATOR,
            doc = "Pulsar web service url that coordinator talks to"
    )
    private String pulsarWebServiceUrl;

    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "The auth plugin used for talking to brokers"
    )
    private String clientAuthenticationPlugin;

    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "The auth plugin used for talking to bookies"
    )
    private String bkClientAuthenticationPlugin;

    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "BookKeeper auth plugin implementation specifics parameters name"
    )
    private String bkClientAuthenticationParametersName;

    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "BookKeeper auth plugin implementation specifics parameters value"
    )
    private String bkClientAuthenticationParametersValue;

    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "Path for the trusted TLS certificate file"
    )
    private String tlsTrustCertsFilePath = "";

    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "Accept untrusted TLS certificate from client"
    )
    private boolean allowTlsInsecureConnection = false;

    @FieldContext(
            category = CATEGORY_SECURITY,
            doc = "Enable hostname verification on TLS connections"
    )
    private boolean enableTlsHostnameVerification = false;

    @FieldContext(
            category = CATEGORY_COORDINATOR,
            doc = "The number of replicas for transaction metadata store"
    )
    private int numCoordinatorMetaStoreReplicas;

    @Override
    public Properties getProperties() {
        return null;
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
