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
import java.time.LocalDate;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for `java.time.LocalDate`.
 */
public class LocalDateSchema extends AbstractSchema<LocalDate> {

   private static final LocalDateSchema INSTANCE;
   private static final SchemaInfo SCHEMA_INFO;

   static {
       SCHEMA_INFO = SchemaInfoImpl.builder()
             .name("LocalDate")
             .type(SchemaType.LOCAL_DATE)
             .schema(new byte[0]).build();
       INSTANCE = new LocalDateSchema();
   }

   public static LocalDateSchema of() {
      return INSTANCE;
   }

   @Override
   public byte[] encode(LocalDate message) {
      if (null == message) {
         return null;
      }

      Long epochDay = message.toEpochDay();
      return LongSchema.of().encode(epochDay);
   }

   @Override
   public LocalDate decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }

      Long decode = LongSchema.of().decode(bytes);
      return LocalDate.ofEpochDay(decode);
   }

   @Override
   public LocalDate decode(ByteBuf byteBuf) {
      if (null == byteBuf) {
         return null;
      }

      Long decode = LongSchema.of().decode(byteBuf);
      return LocalDate.ofEpochDay(decode);
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
