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
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.util.BaseGenerateDocumentation;

@Getter
@Parameters(commandDescription = "Generate documentation automatically.")
@Slf4j
public class CmdGenerateDocumentation extends BaseGenerateDocumentation {

    @Parameter(names = {"-n", "--command-names"}, description = "List of command names")
    private List<String> commandNames = new ArrayList<>();

    public int run() throws PulsarClientException {
        PulsarClientTool pulsarClientTool = new PulsarClientTool(new Properties());
        JCommander commander = pulsarClientTool.jcommander;
        if (commandNames.size() == 0) {
            for (Map.Entry<String, JCommander> cmd : commander.getCommands().entrySet()) {
                if (cmd.getKey().equals("generate_documentation")) {
                    continue;
                }
                generateDocument(cmd.getKey(), commander);
            }
        } else {
            for (String commandName : commandNames) {
                if (commandName.equals("generate_documentation")) {
                    continue;
                }
                generateDocument(commandName, commander);
            }
        }
        return 0;
    }

    protected String generateDocument(String module, JCommander parentCmd) {
        StringBuilder sb = new StringBuilder();
        JCommander cmd = parentCmd.getCommands().get(module);
        sb.append("## ").append(module).append("\n\n");
        sb.append(parentCmd.getUsageFormatter().getCommandDescription(module)).append("\n");
        sb.append("\n\n```shell\n")
                .append("$ pulsar-client ").append(module).append(" [options]")
                .append("\n```");
        sb.append("\n\n");
        sb.append("|Flag|Description|Default|\n");
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

    @Override
    protected String generateDocumentByClassName(String className) throws Exception {
        StringBuilder sb = new StringBuilder();
        if (ClientConfigurationData.class.getName().equals(className)) {
            return generateDocByApiModelProperty(className, "Client", sb);
        } else if (ProducerConfigurationData.class.getName().equals(className)) {
            return generateDocByApiModelProperty(className, "Producer", sb);
        } else if (ConsumerConfigurationData.class.getName().equals(className)) {
            return generateDocByApiModelProperty(className, "Consumer", sb);
        } else if (ReaderConfigurationData.class.getName().equals(className)) {
            return generateDocByApiModelProperty(className, "Reader", sb);
        }
        return "Class [" + className + "] not found";
    }

    public static void main(String[] args) throws Exception {
        CmdGenerateDocumentation generateDocumentation = new CmdGenerateDocumentation();
        generateDocumentation.run(args);
    }
}
