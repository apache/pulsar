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
package org.apache.pulsar.io.core;

import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SourceTest {
    public static class TestSource implements Source<String> {

        @Override
        public void close() throws Exception {

        }

        @Override
        public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
            sourceContext.recordMetric("foo", 1);
        }

        @Override
        public Record<String> read() throws Exception {
            return null;
        }
    }

    @Test
    public void testSinkContext() throws Exception {
        SourceContext sourceContext = mock(SourceContext.class);

        Source testSource = spy(TestSource.class);
        testSource.open(new HashMap<>(), sourceContext);

        verify(sourceContext, times(1)).recordMetric("foo", 1);
    }
}
