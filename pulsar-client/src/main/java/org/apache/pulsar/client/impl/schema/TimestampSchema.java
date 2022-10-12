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
import java.sql.Timestamp;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for `java.sql.Timestamp`.
 */
public class TimestampSchema extends AbstractSchema<Timestamp> {

   private static final TimestampSchema INSTANCE;
   private static final SchemaInfo SCHEMA_INFO;

   static {
       SCHEMA_INFO = SchemaInfoImpl.builder()
             .name("Timestamp")
             .type(SchemaType.TIMESTAMP)
             .schema(new byte[0]).build();
       INSTANCE = new TimestampSchema();
   }

   public static TimestampSchema of() {
      return INSTANCE;
   }

   @Override
   public byte[] encode(Timestamp message) {
      if (null == message) {
         return null;
      }

      Long timestamp = message.getTime();
      return LongSchema.of().encode(timestamp);
   }

   @Override
   public Timestamp decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }

      Long decode = LongSchema.of().decode(bytes);
      return new Timestamp(decode);
   }

   @Override
   public Timestamp decode(ByteBuf byteBuf) {
      if (null == byteBuf) {
         return null;
      }

      Long decode = LongSchema.of().decode(byteBuf);
      return new Timestamp(decode);
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
