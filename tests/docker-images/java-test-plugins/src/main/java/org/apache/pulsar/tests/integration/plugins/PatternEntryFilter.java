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
package org.apache.pulsar.tests.integration.plugins;

import java.util.regex.Pattern;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.KeyValue;

public class PatternEntryFilter implements EntryFilter {

    public static final String FILTER_PATTERN = "entry_filter_pattern";
    public static final String FILTER_PROPERTY = "filter_property";

    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        Pattern p = getPattern(context);
        String value = getMessagePropertyValue(context);
        if (p == null || value == null || p.matcher(value).matches()) {
            return FilterResult.ACCEPT;
        }
        return FilterResult.REJECT;
    }

    private Pattern getPattern(FilterContext context) {
        String subscriptionRegex = context.getSubscription().getSubscriptionProperties().get(FILTER_PATTERN);
        if (subscriptionRegex == null) {
            return null;
        }
        return Pattern.compile(subscriptionRegex);
    }

    private String getMessagePropertyValue(FilterContext context) {
        return context.getMsgMetadata().getPropertiesList().stream()
                .filter(kv -> FILTER_PROPERTY.equals(kv.getKey()))
                .map(KeyValue::getValue)
                .findFirst().orElse(null);
    }

    @Override
    public void close() {
        // Nothing to do here
    }
}
