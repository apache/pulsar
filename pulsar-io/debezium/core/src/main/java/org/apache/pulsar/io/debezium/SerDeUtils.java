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

package org.apache.pulsar.io.debezium;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public class SerDeUtils {
    public static Object deserialize(String objectBase64Encoded) {
        byte[] data = Base64.getDecoder().decode(objectBase64Encoded);
        InputStream bai = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bai)) {
           return ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize the pulsar client to store debezium database history", e);
        }
    }

    public static String serialize(Object obj) throws Exception {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bao)) {
            oos.writeObject(obj);
            oos.flush();
            byte[] data = bao.toByteArray();
            return Base64.getEncoder().encodeToString(data);
        }
    }
}
