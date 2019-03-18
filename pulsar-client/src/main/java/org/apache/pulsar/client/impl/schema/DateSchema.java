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
package org.apache.pulsar.client.impl.schema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.joda.time.LocalDate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * A schema for `org.joda.time.LocalDate`.
 */
public class DateSchema implements Schema<LocalDate> {
   public static DateSchema of() {
      return INSTANCE;
   }

   private static final DateSchema INSTANCE = new DateSchema();
   private static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
         .setName("Date")
         .setType(SchemaType.DATE)
         .setSchema(new byte[0]);

   @Override
   public byte[] encode(LocalDate message) {
      if (null == message) {
         return null;
      }

      try(ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream objOut = new ObjectOutputStream(bos)) {
         objOut.writeObject(message);
         return bos.toByteArray();
      } catch (IOException e) {
         throw new SchemaSerializationException("Encoded data by DateSchema is an exception");
      }
   }

   @Override
   public LocalDate decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }

      try(ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInputStream objIut = new ObjectInputStream(bis)) {
         return (LocalDate) objIut.readObject();
      } catch (ClassNotFoundException | IOException e) {
         throw new SchemaSerializationException("Decoded data by DateSchema is an exception");
      }
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
