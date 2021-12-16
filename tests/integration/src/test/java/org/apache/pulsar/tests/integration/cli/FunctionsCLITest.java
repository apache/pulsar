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
package org.apache.pulsar.tests.integration.cli;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.WorkerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTestBase;
import org.apache.pulsar.tests.integration.functions.utils.UploadDownloadCommandGenerator;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

@Slf4j
public class FunctionsCLITest extends PulsarFunctionsTestBase {

    //
    // Tests on uploading/downloading function packages.
    //

    public String uploadFunction() throws Exception {
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
        ContainerExecResult output = pulsarCluster.getAnyWorker().execCmd(commands);
        assertEquals(0, output.getExitCode());
        assertTrue(output.getStdout().contains("Uploaded successfully"));
        return bkPkgPath;
    }

    // Flaky Test: https://github.com/apache/pulsar/issues/6179
    // @Test
    public void testUploadDownload() throws Exception {
        String bkPkgPath = uploadFunction();
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
        ContainerExecResult output = container.execCmd(commands);
        assertEquals(0, output.getExitCode());
        assertTrue(output.getStdout().contains("Downloaded successfully"));
        String[] diffCommand = {
            "diff",
            PulsarCluster.ADMIN_SCRIPT,
            localPkgFile
        };
        output = container.execCmd(diffCommand);
        assertEquals(0, output.getExitCode());
        output.assertNoOutput();
    }



}
