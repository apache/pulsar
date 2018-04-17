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
package org.apache.pulsar.replicator.function;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest.Action;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.replicator.api.ReplicatorConfig;
import org.apache.pulsar.replicator.api.ReplicatorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A main light-weight replicator function that manages replication-provider to start consume messages from a specific
 * topic and publishes to target external system.
 *
 */
public class ReplicatorFunction implements Function<ReplicatorTopicData, Boolean> {

    public static final String CONF_BROKER_SERVICE_URL = "brokerServiceUrl";
    public static final String CONF_ZK_SERVER_URL = "zkServerUrl";
    public static final String CONF_REPLICATION_TOPIC_NAME = "replTopicName";
    public static final String CONF_REPLICATION_REGION_NAME = "replRegionName";
    public static final String CONF_REPLICATOR_TYPE = "replType";
    public static final String CONF_REPLICATOR_JAR_NAME = "replJar";
    public static final String CONF_REPLICATOR_MANAGER_CLASS_NAME = "replManagerClassName";

    public static final String CONF_REPLICATOR_TENANT_VAL = "pulsar";
    public static final String CONF_REPLICATOR_CLUSTER_VAL = "global";
    public static final String CONF_REPLICATOR_NAMESPACE_VAL = "replicator";

    private ReplicatorManager replicatorManager = null;

    private String topicName;
    private String regionName;
    protected static final AtomicReferenceFieldUpdater<ReplicatorFunction, State> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(ReplicatorFunction.class, State.class, "state");
    private volatile State state = State.Stopped;

    private enum State {
        Stopped, Starting, Started;
    }

    @Override
    public Boolean process(ReplicatorTopicData data, Context context) throws Exception {
        this.topicName = context.getUserConfigValue(CONF_REPLICATION_TOPIC_NAME)
                .orElseThrow(() -> new IllegalArgumentException(
                        "topic name must be present with config " + CONF_REPLICATION_TOPIC_NAME));
        this.regionName = context.getUserConfigValue(CONF_REPLICATION_REGION_NAME)
                .orElseThrow(() -> new IllegalArgumentException(
                        "region name must be present with config " + CONF_REPLICATION_REGION_NAME));

        boolean isValidRequest = data != null && StringUtils.isNotBlank(data.getTopicName())
                && StringUtils.isNotBlank(data.getRegionName()) && data.getTopicName().equals(this.topicName)
                && data.getRegionName().equals(this.regionName);
        if (!isValidRequest) {
            // this request is not for expected topic
            return false;
        }
        log.info("Received replicator action {} for {}", data.getAction(), this.topicName);

        Action processAction = data.getAction();

        switch (processAction) {
        case Stop:
            if (state == State.Started) {
                log.info("stopping replicator manager {}", this.topicName);
                close();
            }
            break;
        case Restart:
            if (state == State.Started) {
                close();
            }
        case Start:
            if (state == State.Stopped) {
                startReplicator(context);
            }
            break;
        default:
            return false;
        }
        return true;
    }

    @Override
    public void init(Context context) throws Exception {
        startReplicator(context);
    }

    /**
     * Based on Replicator type it initialize the replicator-manager which internally manages replicator-producer.
     * 
     * @param context
     * @throws Exception
     */
    protected void startReplicator(Context context) throws Exception {

        this.topicName = context.getUserConfigValue(CONF_REPLICATION_TOPIC_NAME)
                .orElseThrow(() -> new IllegalArgumentException(
                        "topic name must be present with config " + CONF_REPLICATION_TOPIC_NAME));
        this.regionName = context.getUserConfigValue(CONF_REPLICATION_REGION_NAME)
                .orElseThrow(() -> new IllegalArgumentException(
                        "region name must be present with config " + CONF_REPLICATION_REGION_NAME));

        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            log.info("Replicator on topic {} is already in ", topicName, state);
            return;
        }

        String brokerServiceUrl = context.getUserConfigValue(CONF_BROKER_SERVICE_URL).orElse(null);
        String zkServerUrl = context.getUserConfigValue(CONF_ZK_SERVER_URL).orElse(null);
        String jarPath = context.getUserConfigValue(CONF_REPLICATOR_JAR_NAME).orElse(null);
        String replManagerClassName = context.getUserConfigValue(CONF_REPLICATOR_MANAGER_CLASS_NAME)
                .orElseThrow(() -> new IllegalArgumentException("Replicator manager name can't be empty"));

        log.info("starting replicator {}", topicName);

        ClassLoader classLoader = this.getClass().getClassLoader();
        // sometimes, classLoader couldn't find all classes from jar so, loading jar
        // explicitly to make sure classLoader loads all classes
        jarPath = StringUtils.isNotBlank(jarPath) ? jarPath : getJarPath(replManagerClassName);
        boolean closeClassloader = false;
        try {
            if (StringUtils.isNotBlank(jarPath)) {
                URL[] urls = { new File(jarPath).toURL() };
                classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
                closeClassloader = true;
            }
            replicatorManager = (ReplicatorManager) Class.forName(replManagerClassName, true, classLoader)
                    .getConstructor().newInstance();
            ReplicatorConfig replConfig = new ReplicatorConfig();
            replConfig.setTopicName(topicName);
            replConfig.setBrokerServiceUrl(brokerServiceUrl);
            replConfig.setZkServiceUrl(zkServerUrl);
            replConfig.setRegionName(regionName);
            replicatorManager.start(replConfig);
            log.info("Successfully started replicator manager {}", topicName);
            STATE_UPDATER.set(this, State.Started);
        } catch (Exception e) {
            STATE_UPDATER.set(this, State.Stopped);
            log.error("Failed to start replicator manager for {}-{}", topicName);
            throw e;
        } finally {
            if (closeClassloader) {
                ((URLClassLoader) classLoader).close();
            }
        }
    }

    private String getJarPath(String className) {
        try {
            return Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath();
        } catch (Exception e) {
            log.warn("Couldn't find {} in classpath", className);
        }
        return null;
    }

    @VisibleForTesting
    protected void close() {
        if (STATE_UPDATER.get(this) != State.Started) {
            log.info("Replicator is not started {}", this.topicName);
            return;
        }
        try {
            if (replicatorManager != null) {
                replicatorManager.stop();
                replicatorManager = null;
            }
            STATE_UPDATER.set(this, State.Stopped);
        } catch (Exception e) {
            log.warn("Failed to close replicator for {}", this.topicName);
        }
    }

    @VisibleForTesting
    protected ReplicatorManager getReplicatorManager() {
        return replicatorManager;
    }

    public static String getFunctionTopicName(ReplicatorType replicatorType, String postFix) {
        return String.format("%s://%s/%s/%s-%s", TopicDomain.non_persistent.value(), CONF_REPLICATOR_TENANT_VAL,
                CONF_REPLICATOR_NAMESPACE_VAL, replicatorType.toString(), postFix);
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorFunction.class);
}
