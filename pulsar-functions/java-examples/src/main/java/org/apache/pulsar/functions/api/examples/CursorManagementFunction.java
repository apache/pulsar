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

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * A function that demonstrates how to use pulsar admin client exposed from
 * Function Context. In this particular example, for every input message,
 * the function do reset cursor of current function's subscription to a
 * specified timestamp.
 */
public class CursorManagementFunction implements Function<String, String> {

    @Override
    public String process(String input, Context context) throws Exception {
        PulsarAdmin adminClient = context.getPulsarAdmin();
        if (adminClient != null) {
            String topic = context.getCurrentRecord().getTopicName().isPresent() ?
                    context.getCurrentRecord().getTopicName().get() : null;
            String subName = context.getTenant() + "/" + context.getNamespace() + "/" + context.getFunctionName();
            if (topic != null) {
                // 1578188166 below is a random-pick timestamp
                adminClient.topics().resetCursor(topic, subName, 1578188166);
                return "reset cursor successfully";
            }
        }
        return null;
    }
}
