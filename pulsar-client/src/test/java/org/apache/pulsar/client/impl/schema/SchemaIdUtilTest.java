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
package org.apache.pulsar.client.impl.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import org.testng.annotations.Test;

public class SchemaIdUtilTest {

    private static final byte[] SCHEMA_ID = new byte[]{1, 2, 3};
    private static final byte[] SCHEMA_ID_WITH_VALUE_MAGIC_BYTE = new byte[]{-1, 1, 2, 3};
    private static final byte[] SCHEMA_ID_WITH_KEY_VALUE_MAGIC_BYTE = new byte[]{-2, 1, 2, 3};

    @Test
    public void testAddMagicHeader() {
        assertNull(SchemaIdUtil.addMagicHeader(null, false));
        assertNull(SchemaIdUtil.addMagicHeader(new byte[0], false));
        assertEquals(SchemaIdUtil.addMagicHeader(SCHEMA_ID, false), SCHEMA_ID_WITH_VALUE_MAGIC_BYTE);
        assertEquals(SchemaIdUtil.addMagicHeader(SCHEMA_ID, true), SCHEMA_ID_WITH_KEY_VALUE_MAGIC_BYTE);
    }

    @Test
    public void testRemoveMagicHeader() {
        assertNull(SchemaIdUtil.removeMagicHeader(null));
        assertNull(SchemaIdUtil.removeMagicHeader(new byte[0]));
        assertEquals(SchemaIdUtil.removeMagicHeader(SCHEMA_ID), SCHEMA_ID);
        assertEquals(SchemaIdUtil.removeMagicHeader(SCHEMA_ID_WITH_VALUE_MAGIC_BYTE), SCHEMA_ID);
        assertEquals(SchemaIdUtil.removeMagicHeader(SCHEMA_ID_WITH_KEY_VALUE_MAGIC_BYTE), SCHEMA_ID);
    }

}
