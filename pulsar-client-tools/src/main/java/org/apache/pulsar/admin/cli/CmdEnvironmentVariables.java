package org.apache.pulsar.admin.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;

@Getter
@Parameters(commandDescription = "show environment variables.")
@Slf4j
public class CmdEnvironmentVariables extends CmdBase {
    @Parameter(names = {"PULSAR_LOG_CONF"}, description = "Log4j configuration file")
    private String PULSAR_LOG_CONF="conf/log4j2.yaml";

    @Parameter(names = {"PULSAR_CLIENT_CONF"}, description = "Configuration file for the client")
    private String PULSAR_CLIENT_CONF="conf/client.conf";

    @Parameter(names = {"PULSAR_EXTRA_OPT"}, description = "Extra options passed to the JVM")
    private String PULSAR_EXTRA_OPT;

    @Parameter(names = {"PULSAR_EXTRA_CLASSPATH"}, description = "Extra paths for Pulsar's classpath")
    private String PULSAR_EXTRA_CLASSPATH;

    public CmdEnvironmentVariables(Supplier<PulsarAdmin> admin) {
        super("environment_variables", admin);
        jcommander.addCommand("PULSAR_LOG_CONF",PULSAR_LOG_CONF);
        jcommander.addCommand("PULSAR_CLIENT_CONF",PULSAR_LOG_CONF);
        jcommander.addCommand("PULSAR_EXTRA_OPT", PULSAR_EXTRA_OPT);
        jcommander.addCommand("PULSAR_EXTRA_CLASSPATH",PULSAR_EXTRA_CLASSPATH);
    }

}
