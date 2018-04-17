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
package org.apache.pulsar.functions.instance.functions;

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class JavaFunction<I, O> implements IFunction<I, O> {

    private Function<I, O> function;

    public JavaFunction(Function<I, O> function) {
        this.function = function;
    }

    @Override
    public void open(Context context) throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public O process(I input, Context context) throws Exception {
        return this.function.process(input, context);
    }

    @Override
    public Class<?>[] getTypes() {
        return TypeResolver.resolveRawArguments(Function.class, this.function.getClass());
    }
}
