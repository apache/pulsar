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
package org.apache.pulsar.common.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.testng.annotations.Test;

public class TlsClientAuthModeTest {

    @Test
    public void unsetModeIsDerivedFromDeprecatedSettingAndTlsAuthentication() {
        assertThat(TlsClientAuthMode.resolve(null, true, false)).isEqualTo(TlsClientAuthMode.REQUIRE);
        assertThat(TlsClientAuthMode.resolve(null, true, true)).isEqualTo(TlsClientAuthMode.REQUIRE);
        assertThat(TlsClientAuthMode.resolve(null, false, true)).isEqualTo(TlsClientAuthMode.OPTIONAL);
        assertThat(TlsClientAuthMode.resolve(null, false, false)).isEqualTo(TlsClientAuthMode.NONE);
    }

    @Test
    public void explicitModeWins() {
        for (TlsClientAuthMode mode : TlsClientAuthMode.values()) {
            assertThat(TlsClientAuthMode.resolve(mode, false, true)).isEqualTo(mode);
            assertThat(TlsClientAuthMode.resolve(mode, false, false)).isEqualTo(mode);
        }
        assertThat(TlsClientAuthMode.resolve(TlsClientAuthMode.REQUIRE, true, false))
                .isEqualTo(TlsClientAuthMode.REQUIRE);
    }

    @Test
    public void deprecatedRequireConflictsWithWeakerExplicitMode() {
        assertThatThrownBy(() -> TlsClientAuthMode.resolve(TlsClientAuthMode.NONE, true, false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tlsClientAuthMode=NONE");
        assertThatThrownBy(() -> TlsClientAuthMode.resolve(TlsClientAuthMode.OPTIONAL, true, true))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("tlsClientAuthMode=OPTIONAL");
    }
}
