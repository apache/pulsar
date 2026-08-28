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
package org.apache.pulsar.tls;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Locks the {@link TlsPurpose} identity contract (PIP-478): equality is over {@code (role, name)}, so a
 * purpose resolves to the same purpose→policy map slot regardless of how it was minted.
 */
public class TlsPurposeTest {

    @Test
    public void equalsAndHashCodeOverRoleAndName() {
        assertThat(TlsPurpose.client("x")).isEqualTo(TlsPurpose.client("x"));
        assertThat(TlsPurpose.client("x").hashCode()).isEqualTo(TlsPurpose.client("x").hashCode());
    }

    @Test
    public void purposeIsAStableMapKey() {
        Map<TlsPurpose, String> map = new HashMap<>();
        map.put(TlsPurpose.client("x"), "policy");

        assertThat(map).containsKey(TlsPurpose.client("x"));
        assertThat(map.get(TlsPurpose.client("x"))).isEqualTo("policy");
    }

    @Test
    public void roleAndNameDistinguish() {
        assertThat(TlsPurpose.client("x")).isNotEqualTo(TlsPurpose.server("x"));
        assertThat(TlsPurpose.client("x")).isNotEqualTo(TlsPurpose.client("y"));
    }

    @Test
    public void mintedPurposeCarriesRoleAndName() {
        TlsPurpose server = TlsPurpose.server("broker.internal");
        assertThat(server.role()).isEqualTo(TlsPurpose.Role.SERVER);
        assertThat(server.name()).isEqualTo("broker.internal");
    }
}
