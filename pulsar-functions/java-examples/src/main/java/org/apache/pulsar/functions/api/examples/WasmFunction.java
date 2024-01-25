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

package org.apache.pulsar.functions.api.examples;

import io.github.kawamuray.wasmtime.Func;
import io.github.kawamuray.wasmtime.Store;
import io.github.kawamuray.wasmtime.WasmFunctions;
import io.github.kawamuray.wasmtime.WasmValType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.wasm.AbstractWasmFunction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Example WASM function that does not return any value.
 */
public class WasmFunction extends AbstractWasmFunction<String, String> {
    
    private static final Map<Long, String> RESULTS = new ConcurrentHashMap<>();
    
    @Override
    protected Map<String, Func> initWasmCallJavaFunc(Store<Void> store) {
        Map<String, Func> funcMap = new HashMap<>();
        funcMap.put("get_args", WasmFunctions.wrap(store, WasmValType.I64, WasmValType.I64, WasmValType.I32, WasmValType.I32,
                (argId, addr, len) -> {
                    String config = "hello from java " + argId;
                    System.out.println("java side->" + config);
                    ByteBuffer buf = super.getBuffer();
                    for (int i = 0; i < len && i < config.length(); i++) {
                        buf.put(addr.intValue() + i, (byte) config.charAt(i));
                    }
                    return Math.min(config.length(), len);
                }));
        funcMap.put("put_result", WasmFunctions.wrap(store, WasmValType.I64, WasmValType.I64, WasmValType.I32, WasmValType.I32,
                (argId, addr, len) -> {
                    ByteBuffer buf = super.getBuffer();
                    byte[] bytes = new byte[len];
                    for (int i = 0; i < len; i++) {
                        bytes[i] = buf.get(addr.intValue() + i);
                    }
                    String result = new String(bytes, StandardCharsets.UTF_8);
                    RESULTS.put(argId, result);
                    System.out.println("java side->" + result);
                    return 0;
                }));
        return funcMap;
    }
    
    @Override
    protected String doProcess(String input, Context context, Long argumentId) {
        final String result = RESULTS.get(argumentId);
        System.out.println(result);
        return result;
    }
    
    @Override
    protected Long getArgumentId(String input, Context context) {
        return 0L;
    }
}
