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

package org.apache.pulsar.functions.wasm;


import io.github.kawamuray.wasmtime.Extern;
import io.github.kawamuray.wasmtime.WasmFunctions;
import io.github.kawamuray.wasmtime.WasmValType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.wasm.exception.PulsarWasmException;

/**
 * This is the core interface of the WASM function api.
 *
 * @see org.apache.pulsar.functions.api.Function
 */
public abstract class AbstractWasmFunction<X, T> extends WasmLoader implements Function<X, T> {

    private static final String PROCESS_METHOD_NAME = "process";

    protected static final String INITIALIZE_METHOD_NAME = "initialize";

    protected static final String CLOSE_METHOD_NAME = "close";

    protected static final Map<Long, Argument<?>> ARGUMENTS = new ConcurrentHashMap<>();

    @Override
    public T process(X input, Context context) {
        return super.getWasmExtern(PROCESS_METHOD_NAME)
                .map(process -> {
                    Long argumentId = callWASI(input, context, process);
                    return doProcess(input, context, argumentId);
                })
                .orElseThrow(() -> new PulsarWasmException(
                        PROCESS_METHOD_NAME + " function not found in " + super.getWasmName()));
    }

    private Long callWASI(X input,
                          Context context,
                          Extern process) {
        // call WASI function
        final Long argumentId = getArgumentId(input, context);
        ARGUMENTS.put(argumentId, new Argument<>(input, context));
        // WASI cannot easily pass Java objects like JNI, here we pass Long
        // then we can get the argument by Long
        WasmFunctions.consumer(super.getStore(), process.func(), WasmValType.I64)
                .accept(argumentId);
        ARGUMENTS.remove(argumentId);
        return argumentId;
    }

    protected abstract T doProcess(X input, Context context, Long argumentId);

    protected abstract Long getArgumentId(X input, Context context);

    @Override
    public void initialize(Context context) {
        super.getWasmExtern(INITIALIZE_METHOD_NAME)
                .ifPresent(initialize -> callWASI(null, context, initialize));
    }

    @Override
    public void close() {
        super.getWasmExtern(CLOSE_METHOD_NAME)
                .ifPresent(close -> callWASI(null, null, close));
        super.close();
    }

    protected static class Argument<X> {
        protected X input;
        protected Context context;

        private Argument(X input, Context context) {
            this.input = input;
            this.context = context;
        }
    }
}
