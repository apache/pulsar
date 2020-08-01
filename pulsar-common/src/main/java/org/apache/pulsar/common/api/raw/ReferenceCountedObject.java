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
package org.apache.pulsar.common.api.raw;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;

import java.util.function.Consumer;

/**
 * Class representing a reference-counted object that requires explicit deallocation.
 *
 * @param <T> type of the object that requires explicit deallocation.
 */
public class ReferenceCountedObject<T> extends AbstractReferenceCounted {

    private final T object;
    private final Consumer<T> destructor;

    public ReferenceCountedObject(T object, Consumer<T> destructor) {
        this.object = object;
        this.destructor = destructor;
    }

    public T get() {
        return object;
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    @Override
    protected void deallocate() {
        destructor.accept(object);
    }
}
