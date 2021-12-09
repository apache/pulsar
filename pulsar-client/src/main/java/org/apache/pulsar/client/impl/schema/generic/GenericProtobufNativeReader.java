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
package org.apache.pulsar.client.impl.schema.generic;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.pulsar.client.api.schema.Field;
import java.util.List;


public class GenericProtobufNativeReader extends AbstractGenericProtobufReader {

    public GenericProtobufNativeReader(Descriptors.Descriptor descriptor, byte[] schemaVersion) {
        super(descriptor, schemaVersion);
    }

    public GenericProtobufNativeReader(Descriptors.Descriptor descriptor) {
        super(descriptor);
    }

    @Override
    protected AbstractGenericProtobufRecord instanceRecord(byte[] schemaVersion, Descriptors.Descriptor msgDesc,
                                                           List<Field> fields, DynamicMessage record) {
        return new GenericProtobufNativeRecord(schemaVersion, msgDesc, fields, record);
    }
}
