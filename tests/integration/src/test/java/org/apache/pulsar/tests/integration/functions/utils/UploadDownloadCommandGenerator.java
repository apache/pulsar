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
package org.apache.pulsar.tests.integration.functions.utils;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

@Getter
@Setter
@ToString
public class UploadDownloadCommandGenerator {
    public enum MODE {
        UPLOAD,
        DOWNLOAD,
    };
    private MODE mode;
    private String bkPath;
    private String localPath;
    private String brokerUrl;

    public static UploadDownloadCommandGenerator createUploader(String localPath, String bkPath) {
        return new UploadDownloadCommandGenerator(MODE.UPLOAD, localPath, bkPath);
    }

    public static UploadDownloadCommandGenerator createDownloader(String localPath, String bkPath) {
        return new UploadDownloadCommandGenerator(MODE.DOWNLOAD, localPath, bkPath);
    }

    public UploadDownloadCommandGenerator(MODE mode, String localPath, String bkPath) {
        this.mode = mode;
        this.localPath = localPath;
        this.bkPath = bkPath;
    }

    public void createBrokerUrl(String host, int port) {
        brokerUrl = "pulsar://" + host + ":" + port;
    }

    public String generateCommand() {
        StringBuilder commandBuilder = new StringBuilder().append(PulsarCluster.ADMIN_SCRIPT).append(" functions ");
        if (mode == MODE.UPLOAD) {
            commandBuilder.append(" upload ");
        } else {
            commandBuilder.append(" download ");
        }
        commandBuilder.append(" --path ");
        commandBuilder.append(bkPath);
        if (mode == MODE.UPLOAD) {
            commandBuilder.append(" --sourceFile ");
        } else {
            commandBuilder.append(" --destinationFile ");
        }
        commandBuilder.append(localPath);
        return commandBuilder.toString();
    }
}
