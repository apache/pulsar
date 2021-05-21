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
package org.apache.pulsar.functions.api.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.SerDe;

/**
 * Java Serialization based SerDe
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Slf4j
public class JavaSerDe implements SerDe<Object> {

    public static JavaSerDe of() {
        return INSTANCE;
    }

    private static final JavaSerDe INSTANCE = new JavaSerDe();

    @Override
    public byte[] serialize(Object resultValue) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(resultValue);
            out.flush();
            return bos.toByteArray();
        } catch (Exception ex) {
            log.info("Exception during serialization", ex);
        }
        return null;
    }

    @Override
    public Object deserialize(byte[] data) {
        Object obj = null;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            obj = ois.readObject();
        } catch (Exception ex) {
            log.info("Exception during deserialization", ex);
        }
        return obj;
    }
}