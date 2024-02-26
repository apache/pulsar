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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.Path;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.utils.functions.FunctionArchive;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;

@Slf4j
public class FunctionsManager implements AutoCloseable {
    private TreeMap<String, FunctionArchive> functions;

    @VisibleForTesting
    public FunctionsManager() {
        this.functions = new TreeMap<>();
    }

    public FunctionsManager(WorkerConfig workerConfig) throws IOException {
        this.functions = createFunctions(workerConfig);
    }

    public void addFunction(String functionType, FunctionArchive functionArchive) {
        functions.put(functionType, functionArchive);
    }

    public FunctionArchive getFunction(String functionType) {
        return functions.get(functionType);
    }

    public Path getFunctionArchive(String functionType) {
        return functions.get(functionType).getArchivePath();
    }

    public void reloadFunctions(WorkerConfig workerConfig) throws IOException {
        TreeMap<String, FunctionArchive> oldFunctions = functions;
        this.functions = createFunctions(workerConfig);
        closeFunctions(oldFunctions);
    }

    private static TreeMap<String, FunctionArchive> createFunctions(WorkerConfig workerConfig) throws IOException {
        boolean enableClassloading = workerConfig.getEnableClassloadingOfBuiltinFiles()
                || ThreadRuntimeFactory.class.getName().equals(workerConfig.getFunctionRuntimeFactoryClassName());
        return FunctionUtils.searchForFunctions(workerConfig.getFunctionsDirectory(),
                workerConfig.getNarExtractionDirectory(),
                enableClassloading);
    }

    @Override
    public void close() {
        closeFunctions(functions);
    }

    private void closeFunctions(TreeMap<String, FunctionArchive> functionMap) {
        functionMap.values().forEach(functionArchive -> {
            try {
                functionArchive.close();
            } catch (Exception e) {
                log.warn("Failed to close function archive", e);
            }
        });
        functionMap.clear();
    }
}
