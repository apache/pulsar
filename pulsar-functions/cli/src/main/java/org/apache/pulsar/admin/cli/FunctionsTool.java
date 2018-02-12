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

import java.net.URL;
import java.util.function.BiFunction;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarFunctionsAdmin;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import org.apache.pulsar.client.api.ClientConfiguration;

/**
 * TODO: merge this into {@link PulsarAdminTool}.
 */
public class FunctionsTool extends PulsarAdminTool {

    FunctionsTool(Properties properties) throws Exception {
        super(properties);
        commandMap.put("functions", CmdFunctions.class);
    }

    public static void main(String[] args) throws Exception {
        String configFile = args[0];
        Properties properties = new Properties();

        if (configFile != null) {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(configFile);
                properties.load(fis);
            } finally {
                if (fis != null)
                    fis.close();
            }
        }

        FunctionsTool tool = new FunctionsTool(properties);

        int cmdPos;
        for (cmdPos = 1; cmdPos < args.length; cmdPos++) {
            if (tool.commandMap.containsKey(args[cmdPos])) {
                break;
            }
        }
        ++cmdPos;
        boolean isLocalRun = false;
        if (cmdPos < args.length) {
            isLocalRun = "localrun" == args[cmdPos].toLowerCase();
        }

        BiFunction<URL, ClientConfiguration, ? extends PulsarAdmin> adminFactory;
        if (isLocalRun) {
            // bypass constructing admin client
            adminFactory = (url, config) -> null;
        } else {
            adminFactory = (url, config) -> {
                try {
                    return new PulsarFunctionsAdmin(url, config);
                } catch (Exception ex) {
                    System.err.println(ex.getClass() + ": " + ex.getMessage());
                    System.exit(1);
                    return null;
                }
            };
        }

        if (tool.run(Arrays.copyOfRange(args, 1, args.length), adminFactory)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
