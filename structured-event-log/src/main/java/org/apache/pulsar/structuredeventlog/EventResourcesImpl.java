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
package org.apache.pulsar.structuredeventlog;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class EventResourcesImpl implements EventResources {
    private final EventResourcesImpl parent;
    private List<Object> resources = null;

    public EventResourcesImpl(EventResourcesImpl parent) {
        this.parent = parent;
    }

    @Override
    public EventResources resource(String key, Object value) {
        getResources().add(key);
        getResources().add(value);
        return this;
    }

    @Override
    public EventResources resource(String key, Supplier<String> value) {
        resource(key, (Object)value);
        return this;
    }

    public void copyFrom(EventResourcesImpl other) {
        // can't use forEach because we want to avoid toString at this point
        List<Object> resources = getResources();
        if (other.parent != null) {
            copyFrom(other.parent);
        }

        if (other.resources != null) {
            resources.addAll(other.resources);
        }
    }

    public void forEach(BiConsumer<String, String> process) {
        if (parent != null) {
            parent.forEach(process);
        }
        if (resources != null) {
            forEach(resources, process);
        }
    }

    private List<Object> getResources() {
        if (resources == null) {
            resources = new ArrayList<>(2);
        }
        return resources;
    }

    public static void forEach(List<Object> list, BiConsumer<String, String> process) {
        for (int i = 0; i < list.size() - 1; i += 2) {
            String key = String.valueOf(list.get(i));
            Object value = list.get(i + 1);
            if (value instanceof Supplier) {
                value = ((Supplier<?>)value).get();
            }
            process.accept(key, String.valueOf(value));
        }
    }
}
