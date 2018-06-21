package org.apache.pulsar.tests.integration;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import org.apache.pulsar.tests.PulsarClusterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.time.temporal.ChronoUnit.SECONDS;

public class PulsarStandAloneContainer extends GenericContainer<PulsarStandAloneContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarStandAloneContainer.class);

    private static final int PULSAR_BROKER_PORT = 6650;
    private static final int PULSAR_ADMIN_PORT  = 8080;

    private static final String PULSAR_HOST_NAME = "broker";

    private static final String NAME = "apachepulsar";
    private static final String IMG =  "pulsar-test-latest-version";

    private static final String CONTAINER_NAME_BASE = "pulsar-test";
    private FUNC_MODE func_mode;
    private String containerNameAddition;

    public PulsarStandAloneContainer(String containerName){
        this(containerName, FUNC_MODE.STANDALONE);
    }

    public PulsarStandAloneContainer(String containerName, FUNC_MODE mode){
        this(containerName, mode, NAME + "/" + IMG + ":" + "latest");
    }

    private PulsarStandAloneContainer(final String containerName, final FUNC_MODE mode, final String imageName) {
        super(imageName);
        this.func_mode = mode;
        this.containerNameAddition = containerName;
    }

    @Override
    protected void configure() {
        addExposedPort(PULSAR_BROKER_PORT);
        addExposedPort(PULSAR_ADMIN_PORT);
        addEnv("FUNCTION_MODE", func_mode.toString());
    }

    public List<String> getContainerLog() {
        final List<String> logs = new ArrayList<>();

        LogContainerCmd logContainerCmd = this.dockerClient.logContainerCmd(containerId);
        logContainerCmd.withStdOut(true).withStdErr(true);
        try {
            logContainerCmd.exec(new LogContainerResultCallback() {
                @Override
                public void onNext(Frame item) {
                    logs.add(item.toString());
                }
            }).awaitCompletion();
        } catch (InterruptedException e) {

        }
        return logs;
    }

    public FUNC_MODE getFuncMode(){
        return this.func_mode;
    }

    @Override
    public void start() {
        this.waitStrategy = new LogMessageWaitStrategy()
            .withRegEx(".*Successfully validated clusters on tenant.*\\s")
            .withTimes(1)
            .withStartupTimeout(Duration.of(60, SECONDS));
        this.withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
            @Override
            public void accept(CreateContainerCmd createContainerCmd) {
                createContainerCmd.withHostName(PULSAR_HOST_NAME);
                createContainerCmd.withName(getContainerName());
            }
        });
        this.withExposedPorts(PULSAR_BROKER_PORT, PULSAR_ADMIN_PORT);
        this.withLogConsumer(new Slf4jLogConsumer(LOG));
        this.withCommand("/pulsar/bin/pulsar", "standalone");
        super.start();
        LOG.info("Container Name : " +  this.containerName.substring(1));
    }

    @Override
    public void stop() {
        super.stop();
    }

    public String getContainerName(){
        return CONTAINER_NAME_BASE + "-" + containerNameAddition + "-" + getFuncMode().toString();
    }

    public int getPulsarBrokerPort(){
        return this.getMappedPort(PULSAR_BROKER_PORT);
    }

    public int getPulsarAdminPort(){
        return this.getMappedPort(PULSAR_ADMIN_PORT);
    }

    public String getPulsarUrl() {
        return "pulsar://" + this.getContainerIpAddress() + ":" + this.getPulsarBrokerPort();
    }


}