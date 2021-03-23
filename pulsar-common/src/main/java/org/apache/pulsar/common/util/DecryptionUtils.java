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

package org.apache.pulsar.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.Decryption;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * common decryption tool
 */
public class DecryptionUtils {

    /**
     * return src secret with decrypt
     * @return Decryption class
     */
    public static Decryption getNoneDecryptionClass() {

        return new Decryption() {
            @Override
            public String decrypt(String secret) {
                return secret;
            }
        };
    }

    /**
     * invoke service own decrypt class
     * @param className
     * @return
     */
    public static Decryption getDecryptionClass(String className) {

        if (StringUtils.isBlank(className)) {
            return getNoneDecryptionClass();
        }

        try {
            Class<?> srcClass = Class.forName(className);

            Constructor<?> constructor = srcClass.getConstructor();

            Object o = constructor.newInstance();
            if (o instanceof Decryption) {
                return (Decryption) o;
            }
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException(e.getTargetException());
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("please check you classpath. ");
        }
        throw new IllegalArgumentException("please check your Decryption class name . ");
    }
}
