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

import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.connect.core.PushSource;
import org.apache.pulsar.functions.api.Context;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Slf4j
public class PushSourceFunction<I> implements IFunction<I, I> {

    private PushSource<I> source;

    public PushSourceFunction(PushSource<I> source) {
        this.source = source;
    }

    @Override
    public void open(Context context) throws Exception {
        context.getLogger().info("in open...");

        this.source.setConsumer(new Function<I, CompletableFuture<Void>>() {
            @Override
            public CompletableFuture<Void> apply(I i) {
                context.getLogger().info("calling apply: {}", i);
                return context.publish(context.getOutputTopic(), i);
            }
        });

        this.source.open(context.getUserConfigMap());
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public I process(I input, Context context) throws Exception {
        return null;
    }


    public Class<?>[] getTypes() {
        return TypeResolver.resolveRawArguments(PushSource.class, this.source.getClass());
    }
}
