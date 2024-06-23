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
package org.apache.pulsar.tests.integration.containers;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

/**
 * Abstract Test Container for Pulsar.
 */
@Slf4j
public abstract class PulsarContainer<SelfT extends PulsarContainer<SelfT>> extends ChaosContainer<SelfT> {

    public static final int INVALID_PORT = -1;
    public static final int ZK_PORT = 2181;
    public static final int CS_PORT = 2184;
    public static final int BOOKIE_PORT = 3181;
    public static final int BROKER_PORT = 6650;
    public static final int BROKER_PORT_TLS = 6651;
    public static final int BROKER_HTTP_PORT = 8080;
    public static final int BROKER_HTTPS_PORT = 8081;

    public static final String DEFAULT_IMAGE_NAME = System.getenv().getOrDefault("PULSAR_TEST_IMAGE_NAME",
            "apachepulsar/pulsar-test-latest-version:latest");
    public static final String DEFAULT_HTTP_PATH = "/metrics";
    public static final String PULSAR_3_0_IMAGE_NAME = "apachepulsar/pulsar:3.0.0";
    public static final String PULSAR_2_5_IMAGE_NAME = "apachepulsar/pulsar:2.5.0";
    public static final String PULSAR_2_4_IMAGE_NAME = "apachepulsar/pulsar:2.4.0";
    public static final String PULSAR_2_3_IMAGE_NAME = "apachepulsar/pulsar:2.3.0";
    public static final String PULSAR_2_2_IMAGE_NAME = "apachepulsar/pulsar:2.2.0";
    public static final String PULSAR_2_1_IMAGE_NAME = "apachepulsar/pulsar:2.1.0";
    public static final String PULSAR_2_0_IMAGE_NAME = "apachepulsar/pulsar:2.0.0";

    /**
     * For debugging purposes, it is useful to have the ability to leave containers running.
     * This mode can be activated by setting environment variables
     * PULSAR_CONTAINERS_LEAVE_RUNNING=true and TESTCONTAINERS_REUSE_ENABLE=true
     * After debugging, one can use this command to kill all containers that were left running:
     * docker kill $(docker ps -q --filter "label=pulsarcontainer=true")
     */
    public static final boolean PULSAR_CONTAINERS_LEAVE_RUNNING =
            Boolean.parseBoolean(System.getenv("PULSAR_CONTAINERS_LEAVE_RUNNING"));

    @Getter
    protected final String hostname;
    private final String serviceName;
    private final String serviceEntryPoint;
    private final int servicePort;
    private final int servicePortTls;
    private final int httpPort;
    private final int httpsPort;
    private final String httpPath;

    public PulsarContainer(String clusterName,
                           String hostname,
                           String serviceName,
                           String serviceEntryPoint,
                           int servicePort,
                           int httpPort) {
        this(clusterName, hostname, serviceName, serviceEntryPoint, servicePort, httpPort, "/metrics");
    }

    public PulsarContainer(String clusterName,
                           String hostname,
                           String serviceName,
                           String serviceEntryPoint,
                           int servicePort,
                           int httpPort,
                           String httpPath) {
        this(clusterName, hostname, serviceName, serviceEntryPoint, servicePort, httpPort, httpPath,
                DEFAULT_IMAGE_NAME);
    }

    public PulsarContainer(String clusterName,
                           String hostname,
                           String serviceName,
                           String serviceEntryPoint,
                           int servicePort,
                           int httpPort,
                           String httpPath,
                           String pulsarImageName) {
        this(clusterName, hostname, serviceName, serviceEntryPoint, servicePort, 0, httpPort, 0, httpPath,
                pulsarImageName);
    }

    public PulsarContainer(String clusterName,
                           String hostname,
                           String serviceName,
                           String serviceEntryPoint,
                           int servicePort,
                           int servicePortTls,
                           int httpPort,
                           int httpsPort,
                           String httpPath,
                           String pulsarImageName) {
        super(clusterName, pulsarImageName);
        this.hostname = hostname;
        this.serviceName = serviceName;
        this.serviceEntryPoint = serviceEntryPoint;
        this.servicePort = servicePort;
        this.servicePortTls = servicePortTls;
        this.httpPort = httpPort;
        this.httpsPort = httpsPort;
        this.httpPath = httpPath;

        configureLeaveContainerRunning(this);
    }

    public static void configureLeaveContainerRunning(
            GenericContainer<?> container) {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            // use Testcontainers reuse containers feature to leave the container running
            container.withReuse(true);
            // add label that can be used to find containers that are left running.
            container.withLabel("pulsarcontainer", "true");
            // add a random label to prevent reuse of containers
            container.withLabel("pulsarcontainer.random", UUID.randomUUID().toString());
        }
    }

    @Override
    protected void beforeStop() {
        super.beforeStop();
        if (null != getContainerId()) {
            DockerUtils.dumpContainerDirToTargetCompressed(
                getDockerClient(),
                getContainerId(),
                "/var/log/pulsar"
            );
            try {
                // stop the "tail -f ..." commands started in afterStart method
                // so that shutdown output doesn't clutter logs
                execCmd("/usr/bin/pkill", "tail");
            } catch (Exception e) {
                // will fail if there's no tail running
                log.debug("Cannot run 'pkill tail'", e);
            }
        }
    }

    @Override
    public void stop() {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            log.warn("Ignoring stop due to PULSAR_CONTAINERS_LEAVE_RUNNING=true.");
            return;
        }
        super.stop();
    }

    @Override
    protected void doStop() {
        if (getContainerId() != null) {
            if (serviceEntryPoint.equals("bin/pulsar")) {
                // attempt graceful shutdown using "docker stop"
                dockerClient.stopContainerCmd(getContainerId())
                        .withTimeout(15)
                        .exec();
            } else {
                // use "supervisorctl stop all" for graceful shutdown
                try {
                    ContainerExecResult result = execCmd("/usr/bin/supervisorctl", "stop", "all");
                    log.info("Stopped supervisor services exit code: {}\nstdout: {}\nstderr: {}", result.getExitCode(),
                            result.getStdout(), result.getStderr());
                } catch (Exception e) {
                    log.error("Cannot run 'supervisorctl stop all'", e);
                }
            }
        }
        super.doStop();
    }

    @Override
    public String getContainerName() {
        return clusterName + "-" + hostname;
    }

    @Override
    protected void configure() {
        super.configure();
        if (httpPort > 0) {
            addExposedPorts(httpPort);
        }
        if (httpsPort > 0) {
            addExposedPorts(httpsPort);
        }
        if (servicePort > 0) {
            addExposedPort(servicePort);
        }
        if (servicePortTls > 0) {
            addExposedPort(servicePortTls);
        }
    }

    protected void beforeStart() {}

    protected void afterStart() {}

    @Override
    public void start() {
        if (httpPort > 0 && servicePort < 0) {
            this.waitStrategy = new HttpWaitStrategy()
                .forPort(httpPort)
                .forStatusCode(200)
                .forPath(httpPath)
                .withStartupTimeout(Duration.of(300, SECONDS));
        } else if (httpPort > 0 || servicePort > 0) {
            this.waitStrategy = new HostPortWaitStrategy()
                .withStartupTimeout(Duration.of(300, SECONDS));
        }
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withHostName(hostname);
            createContainerCmd.withName(getContainerName());
            createContainerCmd.withEntrypoint(serviceEntryPoint);
        });

        if (isCodeCoverageEnabled()) {
            configureCodeCoverage();
        }

        beforeStart();
        super.start();
        afterStart();
        log.info("[{}] Start pulsar service {} at container {}", getContainerName(), serviceName, getContainerId());
    }

    protected boolean isCodeCoverageEnabled() {
        return Boolean.getBoolean("integrationtest.coverage.enabled");
    }

    protected void configureCodeCoverage() {
        File coverageDirectory;
        if (System.getProperty("integrationtest.coverage.dir") != null) {
            coverageDirectory = new File(System.getProperty("integrationtest.coverage.dir"));
        } else {
            coverageDirectory = new File("target");
        }

        if (!coverageDirectory.isDirectory()) {
            coverageDirectory.mkdirs();
        }
        withFileSystemBind(coverageDirectory.getAbsolutePath(), "/jacocoDir", BindMode.READ_WRITE);

        String jacocoVersion = System.getProperty("jacoco.version");
        File jacocoAgentJar = new File(System.getProperty("user.home"),
                ".m2/repository/org/jacoco/org.jacoco.agent/" + jacocoVersion + "/" + "org.jacoco.agent-"
                        + jacocoVersion + "-runtime.jar");

        if (jacocoAgentJar.isFile()) {
            try {
                FileUtils.copyFileToDirectory(jacocoAgentJar, coverageDirectory);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            withEnv("OPTS", "-javaagent:/jacocoDir/" + jacocoAgentJar.getName()
                    + "=destfile=/jacocoDir/jacoco_" + getContainerName() + "_" + System.currentTimeMillis() + ".exec"
                    + ",includes=org.apache.pulsar.*:org.apache.bookkeeper.mledger.*"
                    + ",excludes=*.proto.*:*.shade.*:*.shaded.*");
        } else {
            log.error("Cannot find jacoco agent jar from '" + jacocoAgentJar.getAbsolutePath() + "'");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PulsarContainer)) {
            return false;
        }

        PulsarContainer<?> another = (PulsarContainer<?>) o;
        return getContainerId().equals(another.getContainerId())
            && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(
                getContainerId());
    }

    public String getPlainTextServiceUrl() {
        return "pulsar://" + getHost() + ":" + getMappedPort(servicePort);
    }

    public String getServiceUrlTls() {
        return "pulsar+ssl://" + getHost() + ":" + getMappedPort(servicePortTls);
    }

    public String getHttpServiceUrl() {
        return "http://" + getHost() + ":" + getMappedPort(httpPort);
    }

    public String getHttpsServiceUrl() {
        return "https://" + getHost() + ":" + getMappedPort(httpsPort);
    }
}
