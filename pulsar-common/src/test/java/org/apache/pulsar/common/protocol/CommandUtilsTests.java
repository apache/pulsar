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
package org.apache.pulsar.common.protocol;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.testng.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class CommandUtilsTests {

    @Test
    public void testToKeyValueList() {
        List<PulsarApi.KeyValue> keyValues = CommandUtils.toKeyValueList(null);
        Assert.assertNotNull(keyValues);
        Assert.assertTrue(keyValues.isEmpty());

        final Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");

        keyValues = CommandUtils.toKeyValueList(metadata);
        Assert.assertEquals(keyValues.size(), keyValues.size());
        PulsarApi.KeyValue kv = keyValues.get(0);
        final Map.Entry<String, String> entry = metadata.entrySet().iterator().next();
        Assert.assertEquals(kv.getKey(), entry.getKey());
        Assert.assertEquals(kv.getValue(), entry.getValue());
    }

    @Test
    public void testMetadataFromCommandProducer() {
        Map<String, String> metadata = CommandUtils.metadataFromCommand(newCommandProducer(null, null));
        Assert.assertNotNull(metadata);
        Assert.assertTrue(metadata.isEmpty());

        final String key = "key";
        final String value = "value";

        PulsarApi.CommandProducer cmd = newCommandProducer(key, value);
        metadata = CommandUtils.metadataFromCommand(cmd);
        Assert.assertEquals(1, metadata.size());
        final Map.Entry<String, String> entry = metadata.entrySet().iterator().next();
        Assert.assertEquals(key, entry.getKey());
        Assert.assertEquals(value, entry.getValue());
    }

    @Test
    public void testMetadataFromCommandSubscribe() {
        Map<String, String> metadata = CommandUtils.metadataFromCommand(newCommandSubscribe(null, null));
        Assert.assertNotNull(metadata);
        Assert.assertTrue(metadata.isEmpty());

        final String key = "key";
        final String value = "value";

        PulsarApi.CommandSubscribe cmd = newCommandSubscribe(key, value);
        metadata = CommandUtils.metadataFromCommand(cmd);
        Assert.assertEquals(1, metadata.size());
        final Map.Entry<String, String> entry = metadata.entrySet().iterator().next();
        Assert.assertEquals(key, entry.getKey());
        Assert.assertEquals(value, entry.getValue());
    }

    private PulsarApi.CommandProducer newCommandProducer(String key, String value) {
        PulsarApi.CommandProducer.Builder cmd = PulsarApi.CommandProducer.newBuilder()
                .setProducerId(1)
                .setRequestId(1)
                .setTopic("my-topic")
                .setProducerName("producer");

        if (key != null && value != null) {
            cmd.addMetadata(PulsarApi.KeyValue.newBuilder().setKey(key).setValue(value).build());
        }

        return cmd.build();
    }

    private PulsarApi.CommandSubscribe newCommandSubscribe(String key, String value) {
        PulsarApi.CommandSubscribe.Builder cmd = PulsarApi.CommandSubscribe.newBuilder()
                .setConsumerId(1)
                .setRequestId(1)
                .setTopic("my-topic")
                .setSubscription("my-subscription")
                .setSubType(PulsarApi.CommandSubscribe.SubType.Shared);

        if (key != null && value != null) {
            cmd.addMetadata(PulsarApi.KeyValue.newBuilder().setKey(key).setValue(value).build());
        }

        return cmd.build();
    }
}
