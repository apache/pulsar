package org.apache.pulsar.transaction.common.utils;

import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.pulsar.transaction.configuration.CoordinatorConfiguration;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.URI;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Holds utils for Coordinator module.
 */
public class CoordinatorUtils {

    /**
     * Initialize a namespace for DistributedLog.
     * @param zkServers
     * @param ledgersRootPath
     * @return
     * @throws IOException
     */
    public static URI initializeDLNamespace(String zkServers, String ledgersRootPath) throws IOException {
        BKDLConfig dlConfig = new BKDLConfig(zkServers, ledgersRootPath);
        DLMetadata dlMetadata = DLMetadata.create(dlConfig);
        URI dlogUri = URI.create(String.format("distributedlog://%s/pulsar/transaction/coordinator", zkServers));

        try {
            dlMetadata.create(dlogUri);
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == KeeperException.Code.NODEEXISTS) {
                return dlogUri;
            }
            throw e;
        }
        return dlogUri;
    }

    /**
     * Create a config for DistributedLog.
     * @param coordinatorConfiguration
     * @return
     */
    public static DistributedLogConfiguration getDLConf(CoordinatorConfiguration coordinatorConfiguration) {
        int numReplicas = coordinatorConfiguration.getNumCoordinatorMetaStoreReplicas();

        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setWriteLockEnabled(false)
                .setOutputBufferSize(256 * 1024)                  // 256k
                .setPeriodicFlushFrequencyMilliSeconds(0)         // disable periodical flush
                .setImmediateFlushEnabled(false)                  // disable immediate flush
                .setLogSegmentRollingIntervalMinutes(0)           // disable time-based rolling
                .setMaxLogSegmentBytes(Long.MAX_VALUE)            // disable size-based rolling
                .setExplicitTruncationByApplication(true)         // no auto-truncation
                .setRetentionPeriodHours(Integer.MAX_VALUE)       // long retention
                .setEnsembleSize(numReplicas)                     // replica settings
                .setWriteQuorumSize(numReplicas)
                .setAckQuorumSize(numReplicas)
                .setUseDaemonThread(true);
        conf.setProperty("bkc.allowShadedLedgerManagerFactoryClass", true);
        conf.setProperty("bkc.shadedLedgerManagerFactoryClassPrefix", "dlshade.");
        if (isNotBlank(coordinatorConfiguration.getBkClientAuthenticationPlugin())) {
            conf.setProperty("bkc.clientAuthProviderFactoryClass",
                    coordinatorConfiguration.getBkClientAuthenticationPlugin());
            if (isNotBlank(coordinatorConfiguration.getBkClientAuthenticationParametersName())) {
                conf.setProperty("bkc." + coordinatorConfiguration.getBkClientAuthenticationParametersName(),
                        coordinatorConfiguration.getBkClientAuthenticationParametersValue());
            }
        }
        return conf;
    }

}
