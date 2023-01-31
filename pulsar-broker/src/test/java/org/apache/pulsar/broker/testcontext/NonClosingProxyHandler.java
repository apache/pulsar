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
package org.apache.pulsar.broker.testcontext;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class NonClosingProxyHandler implements InvocationHandler {
    private final AutoCloseable delegate;

    NonClosingProxyHandler(AutoCloseable delegate) {
        this.delegate = delegate;
    }

    public static <T extends AutoCloseable> T createNonClosingProxy(T delegate, Class<T> interfaceClass) {
        if (isNonClosingProxy(delegate)) {
            return delegate;
        }
        return interfaceClass.cast(Proxy.newProxyInstance(delegate.getClass().getClassLoader(),
                new Class<?>[] {interfaceClass}, new NonClosingProxyHandler(delegate)));
    }

    public static boolean isNonClosingProxy(Object instance) {
        return Proxy.isProxyClass(instance.getClass())
                && Proxy.getInvocationHandler(instance) instanceof NonClosingProxyHandler;
    }

    public static <T extends I, I extends AutoCloseable> I getDelegate(T instance) {
        if (isNonClosingProxy(instance)) {
            return (T) ((NonClosingProxyHandler) Proxy.getInvocationHandler(instance)).getDelegate();
        } else {
            throw new IllegalArgumentException("not a proxy instance with NonClosingProxyHandler");
        }
    }

    public static <T extends AutoCloseable> void reallyClose(T instance) throws Exception {
        if (isNonClosingProxy(instance)) {
            getDelegate(instance).close();
        } else {
            instance.close();
        }
    }

    public AutoCloseable getDelegate() {
        return delegate;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("close")) {
            return null;
        } else {
            return method.invoke(delegate, args);
        }
    }
}
