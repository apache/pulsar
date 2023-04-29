package org.apache.pulsar.broker.configuration;

import static org.apache.pulsar.broker.ServiceConfiguration.CATEGORY_STORAGE_BK;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;



@Getter
@Setter
@ToString
public class ServerBookkeeperClientConfiguration implements PulsarConfiguration {
    private ServiceConfiguration serviceConfiguration;
    /**** --- BookKeeper Client. --- ****/
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Metadata service uri that bookkeeper is used for loading corresponding metadata driver"
                    + " and resolving its metadata service location"
    )
    @Getter(AccessLevel.NONE)
    private String bookkeeperMetadataServiceUri;

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Authentication plugin to use when connecting to bookies"
    )
    private String bookkeeperClientAuthenticationPlugin;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "BookKeeper auth plugin implementation specifics parameters name and values"
    )
    private String bookkeeperClientAuthenticationParametersName;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Parameters for bookkeeper auth plugin"
    )
    private String bookkeeperClientAuthenticationParameters;

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Timeout for BK add / read operations"
    )
    private long bookkeeperClientTimeoutInSeconds = 30;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Speculative reads are initiated if a read request doesn't complete within"
                    + " a certain time Using a value of 0, is disabling the speculative reads")
    private int bookkeeperClientSpeculativeReadTimeoutInMillis = 0;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Number of channels per bookie"
    )
    private int bookkeeperNumberOfChannelsPerBookie = 16;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_STORAGE_BK,
            doc = "Use older Bookkeeper wire protocol with bookie"
    )
    private boolean bookkeeperUseV2WireProtocol = true;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Enable bookies health check. \n\n Bookies that have more than the configured"
                    + " number of failure within the interval will be quarantined for some time."
                    + " During this period, new ledgers won't be created on these bookies")
    private boolean bookkeeperClientHealthCheckEnabled = true;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Bookies health check interval in seconds"
    )
    private long bookkeeperClientHealthCheckIntervalSeconds = 60;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Bookies health check error threshold per check interval"
    )
    private long bookkeeperClientHealthCheckErrorThresholdPerInterval = 5;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Bookie health check quarantined time in seconds"
    )
    private long bookkeeperClientHealthCheckQuarantineTimeInSeconds = 1800;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "bookie quarantine ratio to avoid all clients quarantine "
                    + "the high pressure bookie servers at the same time"
    )
    private double bookkeeperClientQuarantineRatio = 1.0;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Enable rack-aware bookie selection policy. \n\nBK will chose bookies from"
                    + " different racks when forming a new bookie ensemble")
    private boolean bookkeeperClientRackawarePolicyEnabled = true;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Enable region-aware bookie selection policy. \n\nBK will chose bookies from"
                    + " different regions and racks when forming a new bookie ensemble")
    private boolean bookkeeperClientRegionawarePolicyEnabled = false;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Minimum number of racks per write quorum. \n\nBK rack-aware bookie selection policy will try to"
                    + " get bookies from at least 'bookkeeperClientMinNumRacksPerWriteQuorum' "
                    + "racks for a write quorum.")
    private int bookkeeperClientMinNumRacksPerWriteQuorum = 2;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Enforces rack-aware bookie selection policy to pick bookies from "
                    + "'bookkeeperClientMinNumRacksPerWriteQuorum' racks for  a writeQuorum. \n\n"
                    + "If BK can't find bookie then it would throw BKNotEnoughBookiesException "
                    + "instead of picking random one.")
    private boolean bookkeeperClientEnforceMinNumRacksPerWriteQuorum = false;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Enable/disable reordering read sequence on reading entries")
    private boolean bookkeeperClientReorderReadSequenceEnabled = false;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            required = false,
            doc = "Enable bookie isolation by specifying a list of bookie groups to choose from. \n\n"
                    + "Any bookie outside the specified groups will not be used by the broker")
    private String bookkeeperClientIsolationGroups;
    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            required = false,
            doc = "Enable bookie secondary-isolation group if bookkeeperClientIsolationGroups doesn't have enough "
                    + "bookie available."
    )
    private String bookkeeperClientSecondaryIsolationGroups;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Set the interval to periodically check bookie info")
    private int bookkeeperClientGetBookieInfoIntervalSeconds = 60 * 60 * 24; // defaults to 24 hours

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Set the interval to retry a failed bookie info lookup")
    private int bookkeeperClientGetBookieInfoRetryIntervalSeconds = 60;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Enable/disable having read operations for a ledger to be "
            + "sticky to a single bookie.\n"
            + "If this flag is enabled, the client will use one single bookie (by "
            + "preference) to read all entries for a ledger.")
    private boolean bookkeeperEnableStickyReads = true;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Set the client security provider factory class name. "
            + "Default: org.apache.bookkeeper.tls.TLSContextFactory")
    private String bookkeeperTLSProviderFactoryClass = "org.apache.bookkeeper.tls.TLSContextFactory";

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Enable tls authentication with bookie")
    private boolean bookkeeperTLSClientAuthentication = false;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Supported type: PEM, JKS, PKCS12. Default value: PEM")
    private String bookkeeperTLSKeyFileType = "PEM";

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Supported type: PEM, JKS, PKCS12. Default value: PEM")
    private String bookkeeperTLSTrustCertTypes = "PEM";

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path to file containing keystore password, "
            + "if the client keystore is password protected.")
    private String bookkeeperTLSKeyStorePasswordPath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path to file containing truststore password, "
            + "if the client truststore is password protected.")
    private String bookkeeperTLSTrustStorePasswordPath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path for the TLS private key file")
    private String bookkeeperTLSKeyFilePath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path for the TLS certificate file")
    private String bookkeeperTLSCertificateFilePath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Path for the trusted TLS certificate file")
    private String bookkeeperTLSTrustCertsFilePath;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Tls cert refresh duration at bookKeeper-client in seconds (0 "
            + "to disable check)")
    private int bookkeeperTlsCertFilesRefreshDurationSeconds = 300;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Enable/disable disk weight based placement. Default is false")
    private boolean bookkeeperDiskWeightBasedPlacementEnabled = false;

    @FieldContext(category = CATEGORY_STORAGE_BK, doc = "Set the interval to check the need for sending an explicit "
            + "LAC")
    private int bookkeeperExplicitLacIntervalInMills = 0;

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "whether expose managed ledger client stats to prometheus"
    )
    private boolean bookkeeperClientExposeStatsToPrometheus = false;

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "whether limit per_channel_bookie_client metrics of bookkeeper client stats"
    )
    private boolean bookkeeperClientLimitStatsLogging = false;

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Throttle value for bookkeeper client"
    )
    private int bookkeeperClientThrottleValue = 0;

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Number of BookKeeper client worker threads. Default is Runtime.getRuntime().availableProcessors()"
    )
    private int bookkeeperClientNumWorkerThreads = Runtime.getRuntime().availableProcessors();

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Number of BookKeeper client IO threads. Default is Runtime.getRuntime().availableProcessors() * 2"
    )
    private int bookkeeperClientNumIoThreads = Runtime.getRuntime().availableProcessors() * 2;

    @FieldContext(
            category = CATEGORY_STORAGE_BK,
            doc = "Use separated IO threads for BookKeeper client. Default is false, which will use Pulsar IO threads"
    )
    private boolean bookkeeperClientSeparatedIoThreadsEnabled = false;

    @ToString.Exclude
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Properties properties = new Properties();

    public Object getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
