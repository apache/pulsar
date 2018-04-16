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
package org.apache.pulsar.replicator.function.utils;

import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest.Action;
import org.apache.pulsar.replicator.function.ReplicatorTopicData;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 */
public class ReplicatorTopicDataSerDeTest {

    @Test
    public void testSerDe() throws Exception {
        ReplicatorTopicDataSerDe serDe = ReplicatorTopicDataSerDe.instance();
        ReplicatorTopicData topicData = new ReplicatorTopicData();
        topicData.setAction(Action.Start);
        topicData.setTopicName("test");
        byte[] outputByte = serDe.serialize(topicData);
        Assert.assertNotNull(outputByte);
        ReplicatorTopicData outputTopicData = serDe.deserialize(outputByte);
        Assert.assertEquals(topicData, outputTopicData);

    }

    @Test
    public void testSerDeNull() throws Exception {
        ReplicatorTopicDataSerDe serDe = ReplicatorTopicDataSerDe.instance();
        byte[] outputByte = serDe.serialize(null);
        Assert.assertNotNull(outputByte);
        ReplicatorTopicData outputTopicData = serDe.deserialize(outputByte);
        Assert.assertEquals(null, outputTopicData);
    }
}
