/*
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
package org.apache.pulsar.broker.service.schema;

import java.util.HexFormat;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.LongSchemaVersion;

/** Carries the conflicting input back to the registry without changing the checker interface. */
final class SchemaComparisonException extends IncompatibleSchemaException {
    private static final long serialVersionUID = 1L;
    private static final int MAX_DIAGNOSTIC_LENGTH = 2048;
    private final transient SchemaData existingSchema;

    SchemaComparisonException(SchemaData existingSchema, IncompatibleSchemaException cause) {
        super(cause.getMessage(), cause);
        this.existingSchema = existingSchema;
    }

    SchemaData existingSchema() {
        return existingSchema;
    }

    IncompatibleSchemaException withVersion(SchemaVersion version) {
        String value = version instanceof LongSchemaVersion longVersion
                ? Long.toString(longVersion.getVersion()) : HexFormat.of().formatHex(version.bytes());
        String message = getMessage();
        int prefixEnd = message.indexOf(':') + 1;
        message = message.substring(0, prefixEnd) + " existingSchemaVersion=" + value + ", "
                + message.substring(prefixEnd).stripLeading();
        if (message.length() > MAX_DIAGNOSTIC_LENGTH) {
            message = message.substring(0, MAX_DIAGNOSTIC_LENGTH - 3) + "...";
        }
        return new IncompatibleSchemaException(message, this);
    }
}
