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
package org.apache.pulsar.functions.api.examples.window;

import java.util.Collection;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;
import org.slf4j.Logger;

/**
 * A function that demonstrates how to redirect logging to a topic.
 * In this particular example, for every input string, the function
 * does some logging. If --logTopic topic is specified, these log statements
 * end up in that specified pulsar topic.
 */
public class LoggingWindowFunction implements WindowFunction<String, Void> {

    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        Logger log = context.getLogger();
        for (Record<String> record : inputs) {
            log.info(record + "-window-log");
        }
        return null;
    }
}
