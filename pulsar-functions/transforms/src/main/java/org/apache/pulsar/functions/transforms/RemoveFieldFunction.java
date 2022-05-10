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
package org.apache.pulsar.functions.transforms;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

/**
 * This function removes a "field" from a message.
 */
@Slf4j
public class RemoveFieldFunction implements Function<GenericObject, Void> {

    private String field;

    @Override
    public void initialize(Context context) {
        this.field = (String) context.getUserConfigValue("field").get();
    }

    @Override
    public Void process(GenericObject genericObject, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        Schema<?> schema = currentRecord.getSchema();
        Object nativeObject = genericObject.getNativeObject();
        log.debug("apply to {} {}", genericObject, nativeObject);
        log.debug("record with schema {} version {} {}", schema,
                currentRecord.getMessage().get().getSchemaVersion(),
                currentRecord);

        TransformResult transformResult = TransformUtils.newTransformResult(schema, nativeObject);
        TransformUtils.dropValueField(field, transformResult);
        TransformUtils.sendMessage(context, transformResult);
        return null;
    }
}