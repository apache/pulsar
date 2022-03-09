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
package org.apache.pulsar.io.elasticsearch;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Optional;
import org.apache.pulsar.functions.api.Record;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IndexNameFormatterTest {
    @DataProvider(name = "indexFormats")
    public Object[][] indexPatternTestData() {
        return new Object[][]{
                new Object[] {"test", "test"},
                new Object[] {"test-%{yyyy}", "test-%{yyyy}"},
                new Object[] {"test-%{+yyyy}", "test-2022"},
                new Object[] {"test-%{+yyyy-MM}", "test-2022-02"},
                new Object[] {"test-%{+yyyy-MM-dd}", "test-2022-02-18"},
                new Object[] {"test-%{+yyyy-MM-dd}-abc", "test-2022-02-18-abc"},
                new Object[] {"%{+yyyy-MM-dd}-abc", "2022-02-18-abc"},
                new Object[] {"%{+yyyy}/%{+MM-dd}", "2022/02-18"},
        };
    }

    @Test(dataProvider = "indexFormats")
    public void testIndexFormats(String format, String result) {
        Record record = Mockito.mock(Record.class);
        when(record.getEventTime()).thenReturn(Optional.of(1645182000000L));
        IndexNameFormatter formatter = new IndexNameFormatter(format);
        assertEquals(formatter.indexName(record), result);
    }
}
