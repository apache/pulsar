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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;

public class EntryFilterProvider {

    /**
     * create entry filter instance.
     */
    public static EntryFilter createEntryFilter(String className) {
        Class<?> entryFilterClass;
        try {
            entryFilterClass = Class.forName(className);
            Object obj = entryFilterClass.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof EntryFilter, "The instance is not an instance of " + className);
            return (EntryFilter) obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
