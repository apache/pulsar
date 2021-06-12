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

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * A schema for `java.time.Instant`.
 */
public class InstantSchema extends AbstractSchema<Instant> {

   private static final InstantSchema INSTANCE;
   private static final SchemaInfo SCHEMA_INFO;

   static {
       SCHEMA_INFO = new SchemaInfoImpl()
             .setName("Instant")
             .setType(SchemaType.INSTANT)
             .setSchema(new byte[0]);
       INSTANCE = new InstantSchema();
   }

   public static InstantSchema of() {
      return INSTANCE;
   }

   @Override
   public byte[] encode(Instant message) {
      if (null == message) {
         return null;
      }
      // Instant is accurate to nanoseconds and requires two value storage.
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
      buffer.putLong(message.getEpochSecond());
      buffer.putInt(message.getNano());
      return buffer.array();
   }

   @Override
   public Instant decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      long epochSecond = buffer.getLong();
      int nanos = buffer.getInt();
      return Instant.ofEpochSecond(epochSecond, nanos);
   }

   @Override
   public Instant decode(ByteBuf byteBuf) {
      if (null == byteBuf) {
         return null;
      }
      long epochSecond = byteBuf.readLong();
      int nanos = byteBuf.readInt();
      return Instant.ofEpochSecond(epochSecond, nanos);
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
