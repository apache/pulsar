/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.admin.cli;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.util.RelativeTimeUtil;

@Parameters(commandDescription = "Operations about clusters")
public class CmdClusters extends CmdBase {

    @Parameters(commandDescription = "List the existing clusters")
    private class List extends CliCommand {
        void run() throws PulsarAdminException {
            print(admin.clusters().getClusters());
        }
    }

    @Parameters(commandDescription = "Get the configuration data for the specified cluster")
    private class Get extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getCluster(cluster));
        }
    }

    @Parameters(commandDescription = "Provisions a new cluster. This operation requires Pulsar super-user privileges")
    private class Create extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--url", description = "service-url", required = true)
        private String serviceUrl;

        @Parameter(names = "--url-secure", description = "service-url for secure connection", required = false)
        private String serviceUrlTls;

        @Parameter(names = "--broker-url", description = "broker-service-url", required = false)
        private String brokerServiceUrl;

        @Parameter(names = "--broker-url-secure", description = "broker-service-url for secure connection", required = false)
        private String brokerServiceUrlTls;

        @Parameter(names = "--proxy-url", description = "Proxy-service url when client would like to connect to broker via proxy.", required = false)
        private String proxyServiceUrl;

        @Parameter(names = "--proxy-protocol", description = "protocol to decide type of proxy routing eg: SNI", required = false)
        private ProxyProtocol proxyProtocol;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().createCluster(cluster,
                    new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls, proxyServiceUrl,
                            proxyProtocol));
        }
    }

    @Parameters(commandDescription = "Update the configuration for a cluster")
    private class Update extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--url", description = "service-url", required = true)
        private String serviceUrl;

        @Parameter(names = "--url-secure", description = "service-url for secure connection", required = false)
        private String serviceUrlTls;

        @Parameter(names = "--broker-url", description = "broker-service-url", required = false)
        private String brokerServiceUrl;

        @Parameter(names = "--broker-url-secure", description = "broker-service-url for secure connection", required = false)
        private String brokerServiceUrlTls;

        @Parameter(names = "--proxy-url", description = "Proxy-service url when client would like to connect to broker via proxy.", required = false)
        private String proxyServiceUrl;

        @Parameter(names = "--proxy-protocol", description = "protocol to decide type of proxy routing eg: SNI", required = false)
        private ProxyProtocol proxyProtocol;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().updateCluster(cluster, new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl,
                    brokerServiceUrlTls, proxyServiceUrl, proxyProtocol));
        }
    }

    @Parameters(commandDescription = "Deletes an existing cluster")
    private class Delete extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-a", "--all" }, description = "Delete all data (tenants) of the cluster\n", required = false)
        private boolean deleteAll = false;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);

            if (deleteAll) {
                for (String tenant : admin.tenants().getTenants()) {
                    for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                        // Partitioned topic's schema must be deleted by deletePartitionedTopic() but not delete() for each partition
                        for (String topic : admin.topics().getPartitionedTopicList(namespace)) {
                            admin.topics().deletePartitionedTopic(topic, true, true);
                        }
                        for (String topic : admin.topics().getList(namespace)) {
                            admin.topics().delete(topic, true, true);
                        }
                        admin.namespaces().deleteNamespace(namespace, true);
                    }
                    admin.tenants().deleteTenant(tenant);
                }
            }

            admin.clusters().deleteCluster(cluster);
        }
    }

    @Parameters(commandDescription = "Update peer cluster names")
    private class UpdatePeerClusters extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--peer-clusters", description = "Comma separated peer-cluster names [Pass empty string \"\" to delete list]", required = true)
        private String peerClusterNames;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            java.util.LinkedHashSet<String> clusters = StringUtils.isBlank(peerClusterNames) ? null
                    : Sets.newLinkedHashSet(Arrays.asList(peerClusterNames.split(",")));
            admin.clusters().updatePeerClusterNames(cluster, clusters);
        }
    }

    @Parameters(commandDescription = "Get list of peer-clusters")
    private class GetPeerClusters extends CliCommand {

        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getPeerClusterNames(cluster));
        }
    }


    @Parameters(commandDescription = "Create a new failure-domain for a cluster. updates it if already created.")
    private class CreateFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            FailureDomain domain = new FailureDomain();
            domain.setBrokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")): null));
            admin.clusters().createFailureDomain(cluster, domainName, domain);
        }
    }

    @Parameters(commandDescription = "Update failure-domain for a cluster. Creates a new one if not exist.")
    private class UpdateFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            FailureDomain domain = new FailureDomain();
            domain.setBrokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")) : null));
            admin.clusters().updateFailureDomain(cluster, domainName, domain);
        }
    }

    @Parameters(commandDescription = "Deletes an existing failure-domain")
    private class DeleteFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().deleteFailureDomain(cluster, domainName);
        }
    }

    @Parameters(commandDescription = "List the existing failure-domains for a cluster")
    private class ListFailureDomains extends CliCommand {

        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getFailureDomains(cluster));
        }
    }

    @Parameters(commandDescription = "Get the configuration brokers of a failure-domain")
    private class GetFailureDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getFailureDomain(cluster, domainName));
        }
    }

    @Parameters(commandDescription = "Set the offload policies for a cluster")
    protected class SetOffloadPolicies extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(
                names = {"--driver", "-d"},
                description = "Driver to use to offload old data to long term storage, " +
                        "(Possible values: S3, aws-s3, google-cloud-storage, filesystem, azureblob)",
                required = true)
        private String driver;

        @Parameter(
                names = {"--region", "-r"},
                description = "The long term storage region, " +
                        "default is s3ManagedLedgerOffloadRegion or gcsManagedLedgerOffloadRegion in broker.conf",
                required = false)
        private String region;

        @Parameter(
                names = {"--bucket", "-b"},
                description = "Bucket to place offloaded ledger into",
                required = true)
        private String bucket;

        @Parameter(
                names = {"--endpoint", "-e"},
                description = "Alternative endpoint to connect to, " +
                        "s3 default is s3ManagedLedgerOffloadServiceEndpoint in broker.conf",
                required = false)
        private String endpoint;

        @Parameter(
                names = {"--aws-id", "-i"},
                description = "AWS Credential Id to use when using driver S3 or aws-s3",
                required = false)
        private String awsId;

        @Parameter(
                names = {"--aws-secret", "-s"},
                description = "AWS Credential Secret to use when using driver S3 or aws-s3",
                required = false)
        private String awsSecret;

        @Parameter(
                names = {"--maxBlockSize", "-mbs"},
                description = "Max block size (eg: 32M, 64M), default is 64MB",
                required = false)
        private String maxBlockSizeStr;

        @Parameter(
                names = {"--readBufferSize", "-rbs"},
                description = "Read buffer size (eg: 1M, 5M), default is 1MB",
                required = false)
        private String readBufferSizeStr;

        @Parameter(
                names = {"--offloadAfterElapsed", "-oae"},
                description = "Offload after elapsed in minutes (or minutes, hours,days,weeks eg: 100m, 3h, 2d, 5w).",
                required = false)
        private String offloadAfterElapsedStr;

        @Parameter(
                names = {"--offloadAfterThreshold", "-oat"},
                description = "Offload after threshold size (eg: 1M, 5M)",
                required = false)
        private String offloadAfterThresholdStr;

        private final String[] DRIVER_NAMES = OffloadPolicies.DRIVER_NAMES;

        public boolean driverSupported(String driver) {
            return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(driver));
        }

        public boolean isS3Driver(String driver) {
            if (StringUtils.isEmpty(driver)) {
                return false;
            }
            return driver.equalsIgnoreCase(DRIVER_NAMES[0]) || driver.equalsIgnoreCase(DRIVER_NAMES[1]);
        }

        public boolean positiveCheck(String paramName, long value) {
            if (value <= 0) {
                throw new ParameterException(paramName + " should not be negative or 0!");
            }
            return true;
        }

        public boolean maxValueCheck(String paramName, long value, long maxValue) {
            if (value > maxValue) {
                throw new ParameterException(paramName + " should not bigger than " + maxValue + "!");
            }
            return true;
        }

        public OffloadPolicies checkAndGeneratePolicies() {
            if (!driverSupported(driver)) {
                val possibleValues = String.join(",", DRIVER_NAMES);
                throw new ParameterException(
                        "The driver " + driver + " is not supported, " +
                                "(Possible values: " + possibleValues + ").");
            }

            if (isS3Driver(driver) && Strings.isNullOrEmpty(region) && Strings.isNullOrEmpty(endpoint)) {
                throw new ParameterException(
                        "Either s3ManagedLedgerOffloadRegion or s3ManagedLedgerOffloadServiceEndpoint must be set"
                                + " if s3 offload enabled");
            }

            int maxBlockSizeInBytes = OffloadPolicies.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
            if (StringUtils.isNotEmpty(maxBlockSizeStr)) {
                long maxBlockSize = validateSizeString(maxBlockSizeStr);
                if (positiveCheck("MaxBlockSize", maxBlockSize)
                        && maxValueCheck("MaxBlockSize", maxBlockSize, Integer.MAX_VALUE)) {
                    maxBlockSizeInBytes = Long.valueOf(maxBlockSize).intValue();
                }
            }

            int readBufferSizeInBytes = OffloadPolicies.DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
            if (StringUtils.isNotEmpty(readBufferSizeStr)) {
                long readBufferSize = validateSizeString(readBufferSizeStr);
                if (positiveCheck("ReadBufferSize", readBufferSize)
                        && maxValueCheck("ReadBufferSize", readBufferSize, Integer.MAX_VALUE)) {
                    readBufferSizeInBytes = Long.valueOf(readBufferSize).intValue();
                }
            }

            Long offloadAfterElapsedInMillis = OffloadPolicies.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;
            if (StringUtils.isNotEmpty(offloadAfterElapsedStr)) {
                Long offloadAfterElapsed = TimeUnit.SECONDS.toMillis(RelativeTimeUtil.parseRelativeTimeInSeconds(offloadAfterElapsedStr));
                if (positiveCheck("OffloadAfterElapsed", offloadAfterElapsed)
                        && maxValueCheck("OffloadAfterElapsed", offloadAfterElapsed, Long.MAX_VALUE)) {
                    offloadAfterElapsedInMillis = offloadAfterElapsed;
                }
            }

            Long offloadAfterThresholdInBytes = OffloadPolicies.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;
            if (StringUtils.isNotEmpty(offloadAfterThresholdStr)) {
                long offloadAfterThreshold = validateSizeString(offloadAfterThresholdStr);
                if (positiveCheck("OffloadAfterThreshold", offloadAfterThreshold)
                        && maxValueCheck("OffloadAfterThreshold", offloadAfterThreshold, Long.MAX_VALUE)) {
                    offloadAfterThresholdInBytes = offloadAfterThreshold;
                }
            }

            OffloadPolicies offloadPolicies = OffloadPolicies.create(driver, region, bucket, endpoint, awsId, awsSecret,
                    maxBlockSizeInBytes, readBufferSizeInBytes, offloadAfterThresholdInBytes,
                    offloadAfterElapsedInMillis);

            return offloadPolicies;
        }

        @Override
        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            val offloadPolicies = checkAndGeneratePolicies();
            admin.clusters().setOffloadPolicies(cluster, offloadPolicies);
        }
    }

    public CmdClusters(PulsarAdmin admin) {
        super("clusters", admin);
        jcommander.addCommand("get", new Get());
        jcommander.addCommand("create", new Create());
        jcommander.addCommand("update", new Update());
        jcommander.addCommand("delete", new Delete());
        jcommander.addCommand("list", new List());
        jcommander.addCommand("update-peer-clusters", new UpdatePeerClusters());
        jcommander.addCommand("get-peer-clusters", new GetPeerClusters());
        jcommander.addCommand("get-failure-domain", new GetFailureDomain());
        jcommander.addCommand("create-failure-domain", new CreateFailureDomain());
        jcommander.addCommand("update-failure-domain", new UpdateFailureDomain());
        jcommander.addCommand("delete-failure-domain", new DeleteFailureDomain());
        jcommander.addCommand("list-failure-domains", new ListFailureDomains());
        jcommander.addCommand("set-offload-policies", new SetOffloadPolicies());
    }

}
