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
package org.apache.pulsar.broker;

/**
 * Help to switch the class loader of current thread to the NarClassLoader, and change it back when it's done.
 * With the help of try-with-resources statement, the code would be cleaner than using try finally every time.
 */
public class ClassLoaderSwitcher implements AutoCloseable {
    private final ClassLoader prevClassLoader;

    public ClassLoaderSwitcher(ClassLoader classLoader) {
        prevClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
    }

    @Override
    public void close() {
        Thread.currentThread().setContextClassLoader(prevClassLoader);
    }
}