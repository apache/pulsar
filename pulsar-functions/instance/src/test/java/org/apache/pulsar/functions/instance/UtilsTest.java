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
package org.apache.pulsar.functions.instance;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.Test;

/**
 * Unit test of {@link Utils}.
 */
public class UtilsTest {

    @Test
    public void testGetSequenceId() {
        long lid = 12345L;
        long eid = 34566L;
        MessageIdImpl id = mock(MessageIdImpl.class);
        when(id.getLedgerId()).thenReturn(lid);
        when(id.getEntryId()).thenReturn(eid);

        assertEquals((lid << 28) | eid, Utils.getSequenceId(id));
    }

    @Test
    public void testGetMessageId() {
        long lid = 12345L;
        long eid = 34566L;
        long sequenceId = (lid << 28) | eid;

        MessageIdImpl id = (MessageIdImpl) Utils.getMessageId(sequenceId);
        assertEquals(lid, id.getLedgerId());
        assertEquals(eid, id.getEntryId());
    }

}
