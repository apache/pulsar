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
package org.apache.pulsar.client.admin.internal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.fail;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.io.SinkConfig;
import org.testng.annotations.Test;

/**
 * Unit tests
 */
public class SinksImplTest {
    
    
    @Test
    public void testCreateSinkWithoutName() throws Exception {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName(null);
        testBadConfig(sinkConfig, err  -> {
             assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("sink name is required"));
        });              
    }
    
    @Test
    public void testCreateSinkWithEmptyName() throws Exception {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setName(null);
        testBadConfig(sinkConfig, err  -> {
             assertThat(err.getCause(), instanceOf(PulsarAdminException.class));
            assertThat(err.getCause().getMessage(), equalTo("sink name is required"));
        });              
    }
    
    private void testBadConfig(SinkConfig sinkConfig, Consumer<ExecutionException> handler) throws Exception {
        WebTarget web = mock(WebTarget.class);
    
        SinksImpl instance = new SinksImpl(web, null, null, 0);
        CompletableFuture<Void> result = instance.createSinkAsync(sinkConfig, "");
        try {
            result.get();
            fail();
        } catch (ExecutionException err) {
           handler.accept(err);
        }
    }
}
