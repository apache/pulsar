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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * A schema for `java.time.LocalDateTime`.
 */
public class LocalDateTimeSchema extends AbstractSchema<LocalDateTime> {

   private static final LocalDateTimeSchema INSTANCE;
   private static final SchemaInfo SCHEMA_INFO;
   public static final String DELIMITER = ":";

   static {
       SCHEMA_INFO = new SchemaInfoImpl()
             .setName("LocalDateTime")
             .setType(SchemaType.LOCAL_DATE_TIME)
             .setSchema(new byte[0]);
       INSTANCE = new LocalDateTimeSchema();
   }

   public static LocalDateTimeSchema of() {
      return INSTANCE;
   }

   @Override
   public byte[] encode(LocalDateTime message) {
      if (null == message) {
         return null;
      }
      //LocalDateTime is accurate to nanoseconds and requires two value storage.
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
      buffer.putLong(message.toLocalDate().toEpochDay());
      buffer.putLong(message.toLocalTime().toNanoOfDay());
      return buffer.array();
   }

   @Override
   public LocalDateTime decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      long epochDay = buffer.getLong();
      long nanoOfDay = buffer.getLong();
      return LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay));
   }

   @Override
   public LocalDateTime decode(ByteBuf byteBuf) {
      if (null == byteBuf) {
         return null;
      }
      long epochDay = byteBuf.getLong(0);
      long nanoOfDay = byteBuf.getLong(8);
      return LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay));
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
