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
package org.apache.pulsar.internal;

import java.lang.reflect.Constructor;
import picocli.CommandLine;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.InitializationException;

// Copied from https://github.com/remkop/picocli/blob/v4.7.5/src/test/java/picocli/InnerClassFactory.java
// The default Picocli factory doesn't support create non-static inner class.
public class InnerClassFactory implements IFactory {
    private final Object outer;
    private final IFactory defaultFactory = CommandLine.defaultFactory();

    public InnerClassFactory(Object outer) {
        this.outer = outer;
    }

    public <K> K create(final Class<K> cls) throws Exception {
        try {
            return defaultFactory.create(cls);
        } catch (Exception ex0) {
            try {
                Constructor<K> constructor = cls.getDeclaredConstructor(outer.getClass());
                return constructor.newInstance(outer);
            } catch (Exception ex) {
                try {
                    @SuppressWarnings("deprecation") // Class.newInstance is deprecated in Java 9
                    K result = cls.newInstance();
                    return result;
                } catch (Exception ex2) {
                    try {
                        Constructor<K> constructor = cls.getDeclaredConstructor();
                        return constructor.newInstance();
                    } catch (Exception ex3) {
                        throw new InitializationException("Could not instantiate " + cls.getName()
                                + " either with or without construction parameter " + outer + ": " + ex, ex);
                    }
                }
            }
        }
    }
}
