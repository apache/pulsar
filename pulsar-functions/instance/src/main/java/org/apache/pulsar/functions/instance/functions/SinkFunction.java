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
import org.apache.pulsar.connect.core.Sink;
import org.apache.pulsar.functions.api.Context;

import java.util.concurrent.CompletableFuture;

public class SinkFunction<I> implements IFunction<I, Void> {

    private org.apache.pulsar.functions.proto.Function.FunctionDetails functionDetails;
    private Sink<I> sink;

    public SinkFunction(Sink<I> sink) {
        this.sink = sink;
    }

    @Override
    public void open(Context context) throws Exception {
        this.sink.open(context.getUserConfigMap());
    }

    @Override
    public void close() throws Exception {
        this.sink.close();
    }

    @Override
    public Void process(I input, Context context) throws Exception {
        CompletableFuture<Void> completableFuture = this.sink.write(input);
        String topic = context.getInputTopics().iterator().next();
        if (completableFuture != null) {
            completableFuture.thenAcceptAsync(o -> context.ack(context.getMessageId(), topic));
        } else {
            context.ack(context.getMessageId(), topic);
        }
        return null;
    }

    @Override
    public Class<?>[] getTypes() {
        return TypeResolver.resolveRawArguments(Sink.class, this.sink.getClass());
    }
}
