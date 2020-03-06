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
package org.apache.pulsar.functions.instance;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.OutputRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.RichFunction;

public class DefaultRichFunctionWrapper<I, O> implements RichFunction<I, O> {
    private static class JavaFuncWrapper<I, O> implements Function<I, O> {
        private final java.util.function.Function<I, O> func;
        JavaFuncWrapper(java.util.function.Function<I, O> func) {
            this.func = func;
        }

        @Override
        public O process(I input, Context context) throws Exception {
            return func.apply(input);
        }
    }

    private final Function<I, O> func;
    DefaultRichFunctionWrapper(Function<I, O> func){
        this.func = func;
    }
    DefaultRichFunctionWrapper(java.util.function.Function<I, O> function) {
        this.func = new JavaFuncWrapper<>(function);
    }

    @Override
    public OutputRecord<I, O> process(Record<I> srcRecord, Context context) throws Exception {
        return new SinkRecord<>(srcRecord, func.process(srcRecord.getValue(), context));
    }
}
