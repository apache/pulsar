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
package org.apache.pulsar.broker.service.plugin;


import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.collections4.MapUtils;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.common.api.proto.KeyValue;

public class EntryFilter2Test implements EntryFilter {
    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        if (context.getMsgMetadata() == null || context.getMsgMetadata().getPropertiesCount() <= 0) {
            return FilterResult.ACCEPT;
        }
        List<KeyValue> list = context.getMsgMetadata().getPropertiesList();
        // filter by subscription properties
        PersistentSubscription subscription = (PersistentSubscription) context.getSubscription();
        if (!MapUtils.isEmpty(subscription.getSubscriptionProperties())) {
            for (KeyValue keyValue : list) {
                if(subscription.getSubscriptionProperties().containsKey(keyValue.getKey())){
                    return FilterResult.ACCEPT;
                }
            }
        }
        return FilterResult.REJECT;
    }

    @Override
    public void close() {

    }
}
