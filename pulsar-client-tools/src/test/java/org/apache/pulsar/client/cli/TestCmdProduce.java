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
package org.apache.pulsar.client.cli;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCmdProduce {

    CmdProduce cmdProduce;

    @BeforeMethod
    public void setUp() {
        cmdProduce = new CmdProduce();
        cmdProduce.updateConfig(null, null, "ws://localhost:8080/");
    }

    @Test
    public void testGetProduceBaseEndPoint() {
        String topicNameV1 = "persistent://public/cluster/default/issue-11067";
        Assert.assertEquals(cmdProduce.getProduceBaseEndPoint(topicNameV1),
                "ws://localhost:8080/ws/producer/persistent/public/cluster/default/issue-11067");
        String topicNameV2 = "persistent://public/default/issue-11067";
        Assert.assertEquals(cmdProduce.getProduceBaseEndPoint(topicNameV2),
                "ws://localhost:8080/ws/v2/producer/persistent/public/default/issue-11067");
    }
}