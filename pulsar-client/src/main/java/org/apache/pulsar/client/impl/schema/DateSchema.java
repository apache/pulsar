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
import java.util.Date;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema for `java.util.Date` or `java.sql.Date`.
 */
public class DateSchema extends AbstractSchema<Date> {

   private static final DateSchema INSTANCE;
   private static final SchemaInfo SCHEMA_INFO;

   static {
       SCHEMA_INFO = SchemaInfoImpl.builder()
             .name("Date")
             .type(SchemaType.DATE)
             .schema(new byte[0]).build();
       INSTANCE = new DateSchema();
   }

   public static DateSchema of() {
      return INSTANCE;
   }

   @Override
   public byte[] encode(Date message) {
      if (null == message) {
         return null;
      }

      Long date = message.getTime();
      return LongSchema.of().encode(date);
   }

   @Override
   public Date decode(byte[] bytes) {
      if (null == bytes) {
         return null;
      }

      Long decode = LongSchema.of().decode(bytes);
      return new Date(decode);
   }

   @Override
   public Date decode(ByteBuf byteBuf) {
      if (null == byteBuf) {
         return null;
      }

      Long decode = LongSchema.of().decode(byteBuf);
      return new Date(decode);
   }

   @Override
   public SchemaInfo getSchemaInfo() {
      return SCHEMA_INFO;
   }
}
