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
package org.apache.pulsar.tests.integration.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

/**
 * This function processes any message with any schema,
 * and outputs the message with the same schema to another topic.
 */
@Slf4j
public class GenericObjectFunction implements Function<GenericObject, Void> {

    @Override
    public Void process(GenericObject genericObject, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        log.info("apply to {} {}", genericObject, genericObject.getNativeObject());
        log.info("record with schema {} {}", currentRecord.getSchema(), currentRecord);
        // do some processing...
        final boolean isStruct;
        switch (currentRecord.getSchema().getSchemaInfo().getType()) {
            case AVRO:
            case JSON:
            case PROTOBUF_NATIVE:
                isStruct = true;
                break;
            default:
                isStruct = false;
                break;
        }
        if (isStruct) {
            // GenericRecord must stay wrapped
            context.newOutputMessage(context.getOutputTopic(), (Schema) currentRecord.getSchema())
                    .value(genericObject).send();
        } else {
            // primitives and KeyValue must be unwrapped
            context.newOutputMessage(context.getOutputTopic(), (Schema) currentRecord.getSchema())
                    .value(genericObject.getNativeObject()).send();
        }
        return null;
    }
}

