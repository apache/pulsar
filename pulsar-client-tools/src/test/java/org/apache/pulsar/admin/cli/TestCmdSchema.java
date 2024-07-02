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
package org.apache.pulsar.admin.cli;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Schemas;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCmdSchema {

    private PulsarAdmin pulsarAdmin;

    private CmdSchemas cmdSchemas;

    private Schemas schemas;

    @BeforeMethod
    public void setup() throws Exception {
        pulsarAdmin = mock(PulsarAdmin.class);
        schemas = mock(Schemas.class);
        when(pulsarAdmin.schemas()).thenReturn(schemas);
        cmdSchemas = spy(new CmdSchemas(() -> pulsarAdmin));
    }

    @Test
    public void testCmdClusterConfigFile() throws Exception {
        String topic = "persistent://tenant/ns1/t1";
        cmdSchemas.run(new String[]{"metadata", topic});
        verify(schemas).getSchemaMetadata(eq(topic));
    }
}
