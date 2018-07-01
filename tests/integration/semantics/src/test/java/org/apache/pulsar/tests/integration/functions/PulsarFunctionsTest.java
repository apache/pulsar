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
package org.apache.pulsar.tests.integration.functions;

import static org.testng.Assert.assertTrue;

import com.google.common.io.Files;
import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.containers.WorkerContainer;
import org.apache.pulsar.tests.integration.functions.utils.UploadDownloadCommandGenerator;
import org.apache.pulsar.tests.topologies.PulsarCluster;
import org.testcontainers.containers.Container.ExecResult;
import org.testng.annotations.Test;

@Slf4j
public class PulsarFunctionsTest extends PulsarFunctionsTestBase {

    @Test
    public String checkUpload() throws Exception {
        String bkPkgPath = String.format("%s/%s/%s",
            "tenant-" + randomName(8),
            "ns-" + randomName(8),
            "fn-" + randomName(8));

        UploadDownloadCommandGenerator generator = UploadDownloadCommandGenerator.createUploader(
            PulsarCluster.ADMIN_SCRIPT,
            bkPkgPath);
        String actualCommand = generator.generateCommand();

        log.info(actualCommand);

        String[] commands = {
            "sh", "-c", actualCommand
        };
        ExecResult output = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(output.getStdout().contains("\"Uploaded successfully\""));
        return bkPkgPath;
    }

    @Test
    public void checkDownload() throws Exception {
        String bkPkgPath = checkUpload();
        String localPkgFile = "/tmp/checkdownload-" + randomName(16);

        UploadDownloadCommandGenerator generator = UploadDownloadCommandGenerator.createDownloader(
                localPkgFile,
                bkPkgPath);
        String actualCommand = generator.generateCommand();

        log.info(actualCommand);

        String[] commands = {
            "sh", "-c", actualCommand
        };
        WorkerContainer container = pulsarCluster.getAnyWorker();
        ExecResult output = container.execCmd(commands);
        assertTrue(output.getStdout().contains("\"Downloaded successfully\""));
        String[] diffCommand = {
            "diff",
            PulsarCluster.ADMIN_SCRIPT,
            localPkgFile
        };
        output = container.execCmd(diffCommand);
        assertTrue(output.getStdout().isEmpty());
        assertTrue(output.getStderr().isEmpty());
    }

}
