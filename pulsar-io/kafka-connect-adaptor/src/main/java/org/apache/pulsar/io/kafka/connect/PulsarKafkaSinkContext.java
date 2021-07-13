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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.ConnectorContext;

@Slf4j
public class PulsarKafkaSinkContext implements ConnectorContext {

    @Override
    public void requestTaskReconfiguration() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void raiseError(Exception e) {
        throw new UnsupportedOperationException("not implemented", e);
    }
}
