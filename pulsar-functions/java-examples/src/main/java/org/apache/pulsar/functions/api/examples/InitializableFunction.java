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
package org.apache.pulsar.functions.api.examples;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;

@Slf4j
public class InitializableFunction extends TypedMessageBuilderPublish {

    private boolean initialized = false;

    @Override
    public void initialize(Context context) throws Exception {
        log.info("function initialize start");
        initialized = true;
    }

    @Override
    public void close() throws Exception {
        log.info("function close start");
        initialized = false;
    }

    @SneakyThrows
    @Override
    public Void process(String input, Context context) {
        if (!initialized) {
            throw new Exception("function not initialized");
        }
        return super.process(input, context);
    }
}
