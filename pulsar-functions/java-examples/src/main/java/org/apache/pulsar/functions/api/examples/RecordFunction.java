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
package org.apache.pulsar.functions.api.examples;

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

public class RecordFunction implements Function<String, Record<String>> {

    @Override
    public Record<String> process(String input, Context context) throws Exception {
        String publishTopic = (String) context.getUserConfigValueOrDefault("publish-topic", "publishtopic");
        String output = String.format("%s!", input);

        Map<String, String> properties = new HashMap<>(context.getCurrentRecord().getProperties());
        context.getCurrentRecord().getTopicName().ifPresent(topic -> properties.put("input_topic", topic));

        return context.<String>newOutputRecordBuilder()
                .destinationTopic(publishTopic)
                .value(output)
                .properties(properties)
                .build();
    }
}
