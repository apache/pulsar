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
package org.apache.pulsar.tests.integration.io;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

public class TestByteStateSource implements Source<byte[]> {

    private SourceContext sourceContext;

    public static final String VALUE_BASE64 = "0a8001127e0a172e6576656e74732e437573746f6d65724372656174656412630a243"
                                              + "2336366666263652d623038342d346631352d616565342d326330643135356131666"
                                              + "36312026e311a3700000000000000000000000000000000000000000000000000000"
                                              + "000000000000000000000000000000000000000000000000000000000";

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        sourceContext.putState("initial", ByteBuffer.wrap(Base64.getDecoder().decode(VALUE_BASE64)));
        this.sourceContext = sourceContext;
    }

    @Override
    public Record<byte[]> read() throws Exception {
        Thread.sleep(50);
        ByteBuffer initial = sourceContext.getState("initial");
        sourceContext.putState("now", initial);
        return initial::array;
    }

    @Override
    public void close() throws Exception {

    }
}