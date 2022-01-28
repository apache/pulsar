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
package org.apache.pulsar.client.impl.auth;

import static org.testng.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.function.Supplier;

import org.testng.annotations.Test;

public class AuthenticationTlsTest {

    /**
     * This test validates if {@link AuthenticationTls} is serializable to prevent future non-serializable changes and also
     * validates that streamProvider can be serializable and user can use AuthenticationTls in serialiazable task.
     * 
     * @throws Exception
     */
    @Test
    public void testSerializableAuthentication() throws Exception {
        SerializableSupplier tlsCertSupplier = new SerializableSupplier("cert");
        SerializableSupplier tlsKeySupplier = new SerializableSupplier("key");
        SerializableSupplier tlsTrustSupplier = new SerializableSupplier("trust");
        AuthenticationTls tls = new AuthenticationTls(tlsCertSupplier, tlsKeySupplier, tlsTrustSupplier);

        // serialize
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(outStream);
        out.writeObject(tls);
        out.flush();
        byte[] outputBytes = outStream.toByteArray();
        out.close();

        // deserialize
        ByteArrayInputStream bis = new ByteArrayInputStream(outputBytes);
        ObjectInput in = new ObjectInputStream(bis);
        AuthenticationTls ts = (AuthenticationTls) in.readObject();
        in.close();

        // read the object and validate the fields
        byte[] cert = new byte[tlsCertSupplier.getData().length];
        byte[] key = new byte[tlsKeySupplier.getData().length];
        byte[] trust = new byte[tlsTrustSupplier.getData().length];
        ts.getCertStreamProvider().get().read(cert);
        ts.getKeyStreamProvider().get().read(key);
        ts.getTrustStoreStreamProvider().get().read(trust);
        assertEquals(cert, tlsCertSupplier.getData());
        assertEquals(key, tlsKeySupplier.getData());
        assertEquals(trust, tlsTrustSupplier.getData());
    }

    public static class SerializableSupplier implements Supplier<ByteArrayInputStream>, Serializable {

        // Make sure, Object of SerializableSupplier is serializable
        private static final long serialVersionUID = 1L;
        private String type;

        public SerializableSupplier(String type) {
            super();
            this.type = type;
        }

        @Override
        public ByteArrayInputStream get() {
            return new ByteArrayInputStream(getData());
        }

        byte[] getData() {
            return ("data-" + type).getBytes();
        }
    }
}
