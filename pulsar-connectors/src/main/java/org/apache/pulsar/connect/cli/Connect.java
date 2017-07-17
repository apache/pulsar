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
package org.apache.pulsar.connect.cli;

import org.apache.pulsar.connect.runtime.ConnectorExecutionException;
import org.apache.pulsar.connect.runtime.ConnectorRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public final class Connect {
    private static final Logger LOG = LoggerFactory.getLogger(Connect.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Must provide a configuration file.");
            System.exit(1);
        }

        final Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(args[0]));
        } catch (IOException ioe) {
            throw new ConnectorExecutionException(ioe);
        }

        // pretty print the configuration
        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry: properties.entrySet()) {
            sb.append(entry.getKey()).append(": ")
                    .append(entry.getValue()).append("\n");
        }
        LOG.info("running connect with properties\n\n{}", sb);

        final ConnectorRunner runner = ConnectorRunner.fromProperties(properties);
        runner.run();
    }
}
