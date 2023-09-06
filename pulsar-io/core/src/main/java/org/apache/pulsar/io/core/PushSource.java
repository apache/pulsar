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
 * Pulsar's Push Source interface. PushSource read data from
 * external sources (database changes, twitter firehose, etc)
 * and publish to a Pulsar topic. The reason its called Push is
 * because PushSources get passed a consumer that they
 * invoke whenever they have data to be published to Pulsar.
 * The lifecycle of a PushSource is to open it passing any config needed
 * by it to initialize(like open network connection, authenticate, etc).
 * A consumer is then to it which is invoked by the source whenever
 * there is data to be published. Once all data has been read, one can use close
 * at the end of the session to do any cleanup
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class PushSource<T> extends AbstractPushSource<T> implements Source<T> {

    @Override
    public Record<T> read() throws Exception {
        return super.readNext();
    }
}
