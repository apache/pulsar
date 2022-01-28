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

package org.apache.pulsar.io.kafka.connect;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.file.FileStreamSinkConnector;

/**
 * A FileStreamSinkConnector for testing that writes data other than just a value, i.e.:
 * key, value, key and value schemas.
 */
public class SchemaedFileStreamSinkConnector extends FileStreamSinkConnector {
    @Override
    public Class<? extends Task> taskClass() {
        return SchemaedFileStreamSinkTask.class;
    }
}
