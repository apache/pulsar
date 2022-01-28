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

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TestPropertySource implements Source<String> {

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
    }

    @Override
    public Record<String> read() throws Exception {
        Thread.sleep(50);
        return new Record<String>() {
            @Override
            public Optional<String> getKey() {
                return Optional.empty();
            }

            @Override
            public String getValue() {
                return "property";
            }
            @Override
            public Map<String, String> getProperties() {
                HashMap<String, String> props = new HashMap<String, String>();
                props.put("hello", "world");
                props.put("foo", "bar");
                return props;
            }
        };
    }

    @Override
    public void close() throws Exception {

    }
}
