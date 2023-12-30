package org.apache.pulsar.client.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;

@Getter
@Parameters(commandDescription = "show environment variables.")
@Slf4j
public class CmdEnvironmentVariables {
    @Parameter(names = {"PULSAR_LOG_CONF"}, description = "Log4j configuration file")
    private String PULSAR_LOG_CONF="conf/log4j2.yaml";

    @Parameter(names = {"PULSAR_CLIENT_CONF"}, description = "Configuration file for the client")
    private String PULSAR_CLIENT_CONF="conf/client.conf";

    @Parameter(names = {"PULSAR_EXTRA_OPT"}, description = "Extra options passed to the JVM")
    private String PULSAR_EXTRA_OPT;

    @Parameter(names = {"PULSAR_EXTRA_CLASSPATH"}, description = "Extra paths for Pulsar's classpath")
    private String PULSAR_EXTRA_CLASSPATH;

    public int run() throws PulsarClientException {
        PulsarClientTool pulsarClientTool = new PulsarClientTool(new Properties());
        JCommander commander = pulsarClientTool.jcommander;
        environmentVariables(PULSAR_LOG_CONF, commander);
        environmentVariables(PULSAR_CLIENT_CONF, commander);
        environmentVariables(PULSAR_EXTRA_OPT, commander);
        environmentVariables(PULSAR_EXTRA_CLASSPATH, commander);
        return 0;
    }

    protected String environmentVariables(String module, JCommander parentCmd) {
        StringBuilder sb = new StringBuilder();
        JCommander cmd = parentCmd.getCommands().get(module);
        sb.append("## ").append(module).append("\n\n");
        sb.append("\n\n");
        sb.append("|Variable|Description|Default|\n");
        sb.append("|---|---|---|\n");
        List<ParameterDescription> options = cmd.getParameters();
        options.stream().filter(ele -> !ele.getParameterAnnotation().hidden()).forEach((option) ->
            sb.append("| `").append(option.getNames())
                .append("` | ").append(option.getDescription().replace("\n", " "))
                .append("|").append(option.getDefault()).append("|\n")
        );
        System.out.println(sb.toString());
        return sb.toString();
    }
}
