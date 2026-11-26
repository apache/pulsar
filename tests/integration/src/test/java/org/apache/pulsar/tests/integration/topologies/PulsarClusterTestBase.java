/*
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
package org.apache.pulsar.tests.integration.topologies;

import static java.util.stream.Collectors.joining;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.IExecutionListener;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;

@CustomLog
@Listeners(PulsarClusterTestBase.SharedClusterListener.class)
public abstract class PulsarClusterTestBase extends PulsarTestBase {
    // The cluster that the last class which shares its cluster left running for the next one
    private static PulsarCluster idleSharedCluster;

    protected final Map<String, String> brokerEnvs = new HashMap<>();
    protected final Map<String, String> bookkeeperEnvs = new HashMap<>();
    protected final Map<String, String> proxyEnvs = new HashMap<>();
    protected final List<Integer> brokerAdditionalPorts = new LinkedList<>();
    protected final List<Integer> bookieAdditionalPorts = new LinkedList<>();

    @Override
    protected final void setup() throws Exception {
        setupCluster();
    }

    @Override
    protected final void cleanup() throws Exception {
        tearDownCluster();
    }

    @DataProvider(name = "ServiceUrlAndTopics")
    public Object[][] serviceUrlAndTopics() {
        return new Object[][] {
                // plain text, persistent topic
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        true,
                },
                // plain text, non-persistent topic
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        false
                }
        };
    }

    @DataProvider(name = "ServiceUrls")
    public Object[][] serviceUrls() {
        return new Object[][] {
                // plain text
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl())
                }
        };
    }

    @DataProvider(name = "ServiceAndAdminUrls")
    public Object[][] serviceAndAdminUrls() {
        return new Object[][] {
                // plain text
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        stringSupplier(() -> getPulsarCluster().getHttpServiceUrl())
                }
        };
    }

    @DataProvider
    public Object[][] serviceUrlAndTopicDomain() {
        return new Object[][] {
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        TopicDomain.persistent
                },
                {
                        stringSupplier(() -> getPulsarCluster().getPlainTextServiceUrl()),
                        TopicDomain.non_persistent
                },
        };
    }

    protected PulsarAdmin pulsarAdmin;

    protected PulsarCluster pulsarCluster;

    // Whether tearDownCluster leaves the cluster running for the next class: this class shares its cluster and isn't
    // the last class on it, the cluster has started, and no test has failed on it
    private boolean clusterReusable;

    public PulsarCluster getPulsarCluster() {
        return pulsarCluster;
    }

    protected static Supplier<String> stringSupplier(Supplier<String> supplier) {
        return supplier;
    }

    public void setupCluster() throws Exception {
        this.setupCluster("");
    }

    public void setupCluster(String namePrefix) throws Exception {
        String suffix = Stream.of(namePrefix, randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));
        // A container's name is "<cluster>-<hostname>", the longest "<cluster>-configuration-store". Docker's DNS
        // gives no reverse lookup answer for a name longer than a DNS label (63 characters), and the ZooKeeper client
        // in every container looks up the server's name from its address before it connects: it would wait 5 s
        String classNamePart = StringUtils.left(this.getClass().getSimpleName(),
                63 - "-configuration-store".length() - "-".length() - suffix.length());
        String clusterName = classNamePart + "-" + suffix;

        PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .brokerEnvs(brokerEnvs)
                .proxyEnvs(proxyEnvs)
                .brokerAdditionalPorts(brokerAdditionalPorts);

        setupCluster(beforeSetupCluster(clusterName, specBuilder).build());
    }

    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName,
            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        return specBuilder;
    }

    protected void beforeStartCluster() throws Exception {
        // no-op
    }

    protected void setupCluster(PulsarClusterSpec spec) throws Exception {
        setupCluster(spec, true);
    }

    /**
     * Whether this class runs on the cluster that the class before it left running, when that class shares its
     * cluster too and asked for the same spec, and leaves its own cluster running for the class after it, rather than
     * each starting and stopping a cluster of its own: about half a minute a class on CI. The cluster is stopped once
     * a class asks for another spec or doesn't share its cluster, a test fails on it, or the tests end.
     *
     * <p>A class that shares its cluster leaves it as the next class expects to find it: it stops, pauses and
     * reconfigures none of the containers, unless it is the last class on the cluster
     * ({@link #isLastClassOnCluster()}), changes no broker or cluster-wide setting, and gives its tenants, namespaces
     * and topics names that no other class uses. Classes that share a cluster run one after the other in the suite,
     * as only the last one's cluster is kept.
     */
    protected boolean isClusterShared() {
        return false;
    }

    /**
     * Whether this class, which shares its cluster ({@link #isClusterShared()}), is the last class to run on it: it
     * runs on the cluster that the class before it left running, and the cluster is stopped after it rather than left
     * running for the next class. A class that pauses or stops containers can share a cluster this way.
     */
    protected boolean isLastClassOnCluster() {
        return false;
    }

    protected void setupCluster(PulsarClusterSpec spec, boolean doInit) throws Exception {
        incrementSetupNumber();
        boolean shareCluster = doInit && isClusterShared();
        pulsarCluster = shareCluster ? takeIdleSharedCluster(spec) : null;
        if (pulsarCluster != null) {
            log.info()
                    .attr("cluster", pulsarCluster.getClusterName())
                    .log("Running on the cluster that the previous test class left running");
        } else {
            log.info()
                    .attr("cluster", spec.clusterName())
                    .attr("with", spec.numBookies())
                    .attr("bookies", spec.numBrokers())
                    .log("Setting up cluster with bookies, brokers");

            pulsarCluster = PulsarCluster.forSpec(spec);

            beforeStartCluster();

            pulsarCluster.start(doInit);
        }

        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        clusterReusable = shareCluster && !isLastClassOnCluster();

        log.info().attr("cluster", pulsarCluster.getClusterName()).log("Cluster is setup");
    }

    public void tearDownCluster() throws Exception {
        markCurrentSetupNumberCleaned();
        if (null != pulsarCluster) {
            if (clusterReusable && !isCurrentSetupFailed()) {
                leaveSharedClusterRunning(pulsarCluster);
            } else {
                pulsarCluster.stop();
            }
        }
        clusterReusable = false;
    }

    // The cluster that the previous class left running, if it has this spec, whatever its name; else that cluster is
    // stopped
    private static synchronized PulsarCluster takeIdleSharedCluster(PulsarClusterSpec spec) {
        PulsarCluster cluster = idleSharedCluster;
        idleSharedCluster = null;
        if (cluster != null && !cluster.getSpec().equals(spec)) {
            cluster.stop();
            return null;
        }
        return cluster;
    }

    private static synchronized void leaveSharedClusterRunning(PulsarCluster cluster) {
        stopIdleSharedCluster();
        idleSharedCluster = cluster;
    }

    private static synchronized void stopIdleSharedCluster() {
        if (idleSharedCluster != null) {
            idleSharedCluster.stop();
            idleSharedCluster = null;
        }
    }

    /**
     * Stops the cluster that a class left running for the next one ({@link PulsarClusterTestBase#isClusterShared()})
     * before a class that doesn't share its cluster runs a method, as that class may start a cluster of its own, and
     * once the tests end.
     */
    public static class SharedClusterListener implements IInvokedMethodListener, IExecutionListener {
        @Override
        public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
            if (!(testResult.getInstance() instanceof PulsarClusterTestBase test && test.isClusterShared())) {
                stopIdleSharedCluster();
            }
        }

        @Override
        public void onExecutionFinish() {
            stopIdleSharedCluster();
        }
    }
}
