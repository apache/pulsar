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
import java.time.LocalTime;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for `java.time.LocalTime`.
 */
public class LocalTimeSchema extends AbstractSchema<LocalTime> {

   private static final LocalTimeSchema INSTANCE;
   private static final SchemaInfo SCHEMA_INFO;

   static {
       SCHEMA_INFO = SchemaInfoImpl.builder()
             .name("LocalTime")
             .type(SchemaType.LOCAL_TIME)
             .schema(new byte[0]).build();
       INSTANCE = new LocalTimeSchema();
   }

   public static LocalTimeSchema of() {
      return INSTANCE;
   }

   @Override
   public byte[] encode(LocalTime message) {
      if (null == message) {
         return null;
      }

      Long nanoOfDay = message.toNanoOfDay();
      return LongSchema.of().encode(nanoOfDay);
   }

   @Override
   public LocalTime decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }

      Long decode = LongSchema.of().decode(bytes);
      return LocalTime.ofNanoOfDay(decode);
   }

   @Override
   public LocalTime decode(ByteBuf byteBuf) {
      if (null == byteBuf) {
         return null;
      }

      Long decode = LongSchema.of().decode(byteBuf);
      return LocalTime.ofNanoOfDay(decode);
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
