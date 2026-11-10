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
package org.apache.pulsar.metadata.impl.oxia;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.metadata.OxiaTestServer;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.annotations.Test;

/**
 * Operations of a store whose Oxia client was closed underneath it fail with
 * {@link MetadataStoreException.AlreadyClosedException}, as the operations of a closed store do.
 */
public class OxiaMetadataStoreClosedClientTest {

    @Test
    public void operationsFailWithAlreadyClosedWhenTheOxiaClientIsClosed() throws Exception {
        @Cleanup("close")
        OxiaContainer oxiaServer = OxiaTestServer.start(1);
        AsyncOxiaClient client = OxiaClientBuilder.create(oxiaServer.getServiceAddress()).asyncClient().get();
        @Cleanup
        OxiaMetadataStore store = new OxiaMetadataStore(client, "test-identity");
        client.close();

        ExecutionException get = expectThrows(ExecutionException.class,
                () -> store.get("/a").get(10, TimeUnit.SECONDS));
        assertTrue(get.getCause() instanceof MetadataStoreException.AlreadyClosedException,
                "unexpected failure: " + get.getCause());
        ExecutionException put = expectThrows(ExecutionException.class,
                () -> store.put("/a", new byte[0], Optional.empty()).get(10, TimeUnit.SECONDS));
        assertTrue(put.getCause() instanceof MetadataStoreException.AlreadyClosedException,
                "unexpected failure: " + put.getCause());
    }
}
