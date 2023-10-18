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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class BeanUtilTest {

    @Test
    public void test() {

        TopicPolicies source = new TopicPolicies();
        Map<String, BacklogQuotaImpl> map = Maps.newHashMap();
        map.put(BacklogQuota.BacklogQuotaType.message_age.name(),
                new BacklogQuotaImpl(1, 1, BacklogQuota.RetentionPolicy.producer_exception));
        source.setBackLogQuotaMap(map);
        List<CommandSubscribe.SubType> list = Arrays.asList(CommandSubscribe.SubType.Shared);
        source.setSubscriptionTypesEnabled(list);
        source.setMaxProducerPerTopic(100);

        TopicPolicies target = new TopicPolicies();
        BeanUtil.copyProperties(source, target, true);
        assertEquals(source, target);

        target = new TopicPolicies();
        target.setMaxConsumerPerTopic(200);
        BeanUtil.copyProperties(source, target, true);
        assertEquals(target.getMaxConsumerPerTopic(), 200);

        target = new TopicPolicies();
        target.setMaxConsumerPerTopic(200);
        BeanUtil.copyProperties(source, target, false);
        assertEquals(target.getMaxConsumerPerTopic(), null);
    }
}
