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
package org.apache.pulsar.io.core;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.Record;

/**
 * Pulsar's Batch Push Source interface. Batch Push Sources have the same lifecycle
 * as the regular BatchSource, aka discover, prepare. The reason its called Push is
 * because BatchPushSource can emit a record using the consume method that they
 * invoke whenever they have data to be published to Pulsar.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class BatchPushSource<T> extends AbstractPushSource<T> implements BatchSource<T> {

    @Override
    public Record<T> readNext() throws Exception {
        return super.readNext();
    }
}
