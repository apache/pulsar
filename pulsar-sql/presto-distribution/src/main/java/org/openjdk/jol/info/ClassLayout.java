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
package org.openjdk.jol.info;

import com.twitter.common.objectsize.ObjectSizeCalculator;
import io.airlift.log.Logger;
import org.objenesis.ObjenesisStd;

/**
 * Mock class avoid a dependency on OpenJDK JOL,
 * which is incompatible with the Apache License.
 */
public class ClassLayout {

    private static final Logger log = Logger.get(ClassLayout.class);

    private int size;
    private static final int DEFAULT_SIZE = 64;

    private ClassLayout(int size) {
        this.size = size;
    }

    public static ClassLayout parseClass(Class<?> clazz) {
        long size = DEFAULT_SIZE;
        try {
            size = ObjectSizeCalculator.getObjectSize(new ObjenesisStd().newInstance(clazz));
        } catch (Throwable th) {
            log.info("Error estimating size of class %s",clazz, th);
        }
        return new ClassLayout(Math.toIntExact(size));
    }

    public int instanceSize() {
        return size;
    }
}