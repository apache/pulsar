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
package org.apache.pulsar.tests.integration.offload;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class TestOffloadDeletionFS extends TestBaseOffload {

    @Override
    protected int getEntrySize() {
        return 512;
    }

    @Override
    protected int getNumEntriesPerLedger() {
        return 200;
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteOffloadedTopic(Supplier<String> serviceUrl, Supplier<String> adminUrl) throws Exception {
        super.testDeleteOffloadedTopic(serviceUrl.get(), adminUrl.get(), false, 0);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteUnloadedOffloadedTopic(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testDeleteOffloadedTopic(serviceUrl.get(), adminUrl.get(), true, 0);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteOffloadedTopicExistsInBk(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testDeleteOffloadedTopicExistsInBk(serviceUrl.get(), adminUrl.get(), false, 0);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteUnloadedOffloadedTopicExistsInBk(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testDeleteOffloadedTopicExistsInBk(serviceUrl.get(), adminUrl.get(), true, 0);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteOffloadedPartitionedTopic(Supplier<String> serviceUrl, Supplier<String> adminUrl) throws Exception {
        super.testDeleteOffloadedTopic(serviceUrl.get(), adminUrl.get(), false, 3);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteUnloadedOffloadedPartitionedTopic(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testDeleteOffloadedTopic(serviceUrl.get(), adminUrl.get(), true, 3);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteOffloadedPartitionedTopicExistsInBk(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testDeleteOffloadedTopicExistsInBk(serviceUrl.get(), adminUrl.get(), false, 3);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testDeleteUnloadedOffloadedPartitionedTopicExistsInBk(Supplier<String> serviceUrl,
                                                                      Supplier<String> adminUrl) throws Exception {
        super.testDeleteOffloadedTopicExistsInBk(serviceUrl.get(), adminUrl.get(), true, 3);
    }

    @Override
    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("managedLedgerMaxEntriesPerLedger", String.valueOf(getNumEntriesPerLedger()));
        result.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        result.put("managedLedgerOffloadDriver", "filesystem");
        result.put("fileSystemURI", "file:///");

        return result;
    }

    @Override
    protected boolean offloadedLedgerExists(String topic, int partitionNum, long ledger) {
        log.info("offloadedLedgerExists(topic = {}, partitionNum={},ledger={})",
                topic, partitionNum, ledger);
        if (partitionNum > -1) {
            topic = topic + "-partition-" + partitionNum;
        }
        String managedLedgerName = TopicName.get(topic).getPersistenceNamingEncoding();
        String rootPath = "pulsar/";
        String dirPath = rootPath + managedLedgerName + "/";

        List<String> result = new LinkedList<>();
        String[] cmds = {
                "ls",
                "-1",
                dirPath
                };
        pulsarCluster.getBrokers().forEach(broker -> {
            try {
                ContainerExecResult res = broker.execCmd(cmds);
                log.info("offloadedLedgerExists broker {} 'ls -1 {}' got {}",
                        broker.getContainerName(), dirPath, res.getStdout());
                Arrays.stream(res.getStdout().split("\n"))
                        .filter(x -> x.startsWith(ledger + "-"))
                        .forEach(x -> result.add(x));
            } catch (ContainerExecException ce) {
                log.info("offloadedLedgerExists broker {} 'ls -1 {}' got error code {}",
                        broker.getContainerName(), dirPath, ce.getResult().getExitCode());
                // ignore 2 (No such file or directory)
                if (ce.getResult().getExitCode() != 2) {
                    throw new RuntimeException(ce);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return !result.isEmpty();
    }

}
