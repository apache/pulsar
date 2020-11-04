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
package org.apache.pulsar.functions.worker;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;
import org.apache.pulsar.functions.utils.functions.Functions;
@Slf4j
public class FunctionsManager {

    private Functions functions;

    public FunctionsManager(WorkerConfig workerConfig) throws IOException {
        this.functions = FunctionUtils.searchForFunctions(workerConfig.getFunctionsDirectory());
    }

    public List<FunctionDefinition> getFunctions() {
        return functions.getFunctionsDefinitions();
    }

    public Path getFunctionArchive(String functionType) {
        return functions.getFunctions().get(functionType);
    }

    public void reloadFunctions(WorkerConfig workerConfig) throws IOException {
        this.functions = FunctionUtils.searchForFunctions(workerConfig.getFunctionsDirectory());
    }
}
