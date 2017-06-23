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
package org.apache.pulsar.utils;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyOnWriteArrayList<T> extends java.util.concurrent.CopyOnWriteArrayList<T> {
    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(CopyOnWriteArrayList.class);
    private static final Method getArrayMethod;

    static {
        try {
            getArrayMethod = java.util.concurrent.CopyOnWriteArrayList.class.getDeclaredMethod("getArray");
            getArrayMethod.setAccessible(true);
        } catch (Exception e) {
            log.error("Error in getting method", e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public T[] array() {
        try {
            return (T[]) getArrayMethod.invoke(this);
        } catch (Exception e) {
            log.error("Error in invoking method", e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "serial" })
    public static final CopyOnWriteArrayList EMPTY_LIST = new CopyOnWriteArrayList() {

        @Override
        public Object set(int index, Object element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(Object e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(int index, Object element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addIfAbsent(Object e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int addAllAbsent(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(int index, Collection c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeIf(Predicate filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceAll(UnaryOperator operator) {
            throw new UnsupportedOperationException();
        }

    };

    @SuppressWarnings("unchecked")
    public static <T> CopyOnWriteArrayList<T> empty() {
        return EMPTY_LIST;
    }
}
