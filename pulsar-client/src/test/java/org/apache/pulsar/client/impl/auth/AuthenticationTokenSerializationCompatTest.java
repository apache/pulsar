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
package org.apache.pulsar.client.impl.auth;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.testng.annotations.Test;

/**
 * The v4 {@code Authentication} interface extends {@link Serializable}, so an {@code AuthenticationToken}
 * serialized by Pulsar 4.x must still deserialize on 5.0. PIP-478 moved the token suppliers onto the v5 body
 * ({@code TokenAuthenticationV5.LiteralTokenSupplier} / {@code FileTokenSupplier}); Java serialization
 * resolves by class name, so the 4.x inner classes are retained as deserialization shims that resolve to the
 * new suppliers. These tests pin that.
 */
public class AuthenticationTokenSerializationCompatTest {

    @Test
    public void aTokenSerializedOnThisVersionRoundTrips() throws Exception {
        AuthenticationToken original = new AuthenticationToken("my-token");
        AuthenticationToken restored = roundTrip(original);
        assertThat(authDataToken(restored)).isEqualTo("my-token");
    }

    @Test
    public void a4xSerializedSupplierStillDeserializes() throws Exception {
        // Reconstruct the exact 4.x wire form: the inner supplier class, under its original name. Losing this
        // shim turns a 4.x blob into a ClassNotFoundException rather than a working plugin.
        Supplier<String> legacy = newLegacySupplier("legacy-token");
        assertThat(legacy.getClass().getName())
                .isEqualTo("org.apache.pulsar.client.impl.auth.AuthenticationToken$SerializableTokenSupplier");

        Object resolved = roundTripObject(legacy);
        assertThat(((Supplier<?>) resolved).get()).isEqualTo("legacy-token");
        // readResolve hands back the v5 supplier, so nothing downstream keeps the deprecated shim alive.
        assertThat(resolved.getClass().getName())
                .isEqualTo("org.apache.pulsar.client.impl.auth.v5.TokenAuthenticationV5$LiteralTokenSupplier");
    }

    @SuppressWarnings("unchecked")
    private static Supplier<String> newLegacySupplier(String token) throws Exception {
        Class<?> cls = Class.forName(
                "org.apache.pulsar.client.impl.auth.AuthenticationToken$SerializableTokenSupplier");
        var ctor = cls.getDeclaredConstructor(String.class);
        ctor.setAccessible(true);
        return (Supplier<String>) ctor.newInstance(token);
    }

    @SuppressWarnings("deprecation")
    private static String authDataToken(AuthenticationToken token) throws Exception {
        AuthenticationDataProvider data = token.getAuthData();
        return data.getCommandData();
    }

    private static AuthenticationToken roundTrip(AuthenticationToken token) throws Exception {
        return (AuthenticationToken) roundTripObject(token);
    }

    private static Object roundTripObject(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return in.readObject();
        }
    }
}
