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
package org.apache.pulsar.metadata.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.UUID;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.annotations.Test;

public class ZKMetadataStoreTest {

    @Test(timeOut = 30_000)
    public void constructorFailureClosesResources() {
        String metadataStoreName = "failed-zk-store-" + UUID.randomUUID();
        MetadataStoreConfig config = MetadataStoreConfig.builder()
                .metadataStoreName(metadataStoreName)
                .sessionTimeoutMillis(100)
                .build();

        assertThatThrownBy(() -> new ZKMetadataStore("127.0.0.1:1", config, false))
                .isInstanceOf(MetadataStoreException.class);

        assertThat(Thread.getAllStackTraces().keySet())
                .filteredOn(Thread::isAlive)
                .extracting(Thread::getName)
                .noneMatch(name -> name.startsWith(metadataStoreName));
    }
}
