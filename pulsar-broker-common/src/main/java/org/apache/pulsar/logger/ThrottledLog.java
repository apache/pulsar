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
package org.apache.pulsar.logger;

import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.Marker;

public class ThrottledLog implements Logger {

    private final Logger delegate;

    /**
     * How many permits per filling.
     */
    private final int permitsPerFilling;

    /**
     * How often should the basket be filled.
     */
    private final long periodInMilliSecond;

    /**
     * The last time of refreshing the bucket of permits.
     * No thread-safe processing is done because a particularly precise count is not required.
     */
    private long lastFillTimestamp;

    /**
     * How many permits are there now.
     * No thread-safe processing is done because a particularly precise count is not required.
     */
    private int permits = 0;

    public ThrottledLog(Logger log, long periodInSecond, int permitsPerPeriod) {
        if (permitsPerPeriod <= 0) {
            throw new IllegalArgumentException("permitsPerPeriod should be larger than 0");
        }
        this.delegate = log;
        this.periodInMilliSecond = periodInSecond * 1000;
        this.permitsPerFilling = permitsPerPeriod;
        this.permits = permitsPerFilling;
        this.lastFillTimestamp = System.currentTimeMillis();
    }

    private boolean acquire() {
        if (System.currentTimeMillis() - lastFillTimestamp > periodInMilliSecond) {
            permits = permitsPerFilling;
            lastFillTimestamp = System.currentTimeMillis();
        }
        return --permits >= 0;
    }

    private Object[] calculateGeneratedArgs(Object... arguments) {
        Object[] args = new Object[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] instanceof Supplier<?>) {
                Supplier<?> supplier = (Supplier<?>) arguments[i];
                args[0] = supplier.get();
            } else {
                args[0] = arguments[i];
            }
        }
        return args;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return delegate.isTraceEnabled(marker);
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return delegate.isDebugEnabled(marker);
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return delegate.isInfoEnabled(marker);
    }

    @Override
    public boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return delegate.isWarnEnabled(marker);
    }

    @Override
    public boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return delegate.isErrorEnabled(marker);
    }

    @Override
    public void trace(String msg) {
        if (acquire()) {
            delegate.trace(msg);
        }
    }

    public void traceWithGeneratedArgs(Supplier<String> msg) {
        if (acquire()) {
            delegate.trace(msg.get());
        }
    }

    @Override
    public void trace(String format, Object arg) {
        if (acquire()) {
            delegate.trace(format, arg);
        }
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.trace(format, arg1, arg2);
        }
    }

    public void traceWithGeneratedArgs(String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.trace(format, arg1.get(), arg2.get());
        }
    }

    public void traceWithGeneratedArgs(String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.trace(format, arg1.get(), arg2);
        }
    }

    public void traceWithGeneratedArgs(String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.trace(format, arg1, arg2.get());
        }
    }

    @Override
    public void trace(String format, Object... arguments) {
        if (acquire()) {
            delegate.trace(format, arguments);
        }
    }

    public void traceWithGeneratedArgs(String format, Object... args) {
        if (acquire()) {
            delegate.trace(format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void trace(String msg, Throwable t) {
        if (acquire()) {
            delegate.trace(msg, t);
        }
    }

    public void traceWithGeneratedArgs(Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.trace(msg.get(), t);
        }
    }

    public void traceWithGeneratedArgs(Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.trace(msg.get(), t.get());
        }
    }

    public void traceWithGeneratedArgs(String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.trace(msg, t.get());
        }
    }

    @Override
    public void trace(Marker marker, String msg) {
        if (acquire()) {
            delegate.trace(marker, msg);
        }
    }

    public void traceWithGeneratedArgs(Marker marker, Supplier<String> msg) {
        if (acquire()) {
            delegate.trace(marker, msg.get());
        }
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        if (acquire()) {
            delegate.trace(marker, format, arg);
        }
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.trace(marker, format, arg1, arg2);
        }
    }

    public void traceWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.trace(marker, format, arg1.get(), arg2.get());
        }
    }

    public void traceWithGeneratedArgs(Marker marker, String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.trace(marker, format, arg1, arg2.get());
        }
    }

    public void traceWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.trace(marker, format, arg1.get(), arg2);
        }
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        if (acquire()) {
            delegate.trace(marker, format, argArray);
        }
    }

    public void traceWithGeneratedArgs(Marker marker, String format, Object... args) {
        if (acquire()) {
            delegate.trace(marker, format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        if (acquire()) {
            delegate.trace(marker, msg, t);
        }
    }

    public void traceWithGeneratedArgs(Marker marker, Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.trace(marker, msg.get(), t.get());
        }
    }

    public void traceWithGeneratedArgs(Marker marker, String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.trace(marker, msg, t.get());
        }
    }

    public void traceWithGeneratedArgs(Marker marker, Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.trace(marker, msg.get(), t);
        }
    }

    @Override
    public void debug(String msg) {
        if (acquire()) {
            delegate.debug(msg);
        }
    }

    public void debugWithGeneratedArgs(Supplier<String> msg) {
        if (acquire()) {
            delegate.debug(msg.get());
        }
    }

    @Override
    public void debug(String format, Object arg) {
        if (acquire()) {
            delegate.debug(format, arg);
        }
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.debug(format, arg1, arg2);
        }
    }

    public void debugWithGeneratedArgs(String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.debug(format, arg1.get(), arg2.get());
        }
    }

    public void debugWithGeneratedArgs(String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.debug(format, arg1.get(), arg2);
        }
    }

    public void debugWithGeneratedArgs(String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.debug(format, arg1, arg2.get());
        }
    }

    @Override
    public void debug(String format, Object... arguments) {
        if (acquire()) {
            delegate.debug(format, arguments);
        }
    }

    public void debugWithGeneratedArgs(String format, Object... args) {
        if (acquire()) {
            delegate.debug(format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void debug(String msg, Throwable t) {
        if (acquire()) {
            delegate.debug(msg, t);
        }
    }

    public void debugWithGeneratedArgs(Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.debug(msg.get(), t);
        }
    }

    public void debugWithGeneratedArgs(Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.debug(msg.get(), t.get());
        }
    }

    public void debugWithGeneratedArgs(String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.debug(msg, t.get());
        }
    }

    @Override
    public void debug(Marker marker, String msg) {
        if (acquire()) {
            delegate.debug(marker, msg);
        }
    }

    public void debugWithGeneratedArgs(Marker marker, Supplier<String> msg) {
        if (acquire()) {
            delegate.debug(marker, msg.get());
        }
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        if (acquire()) {
            delegate.debug(marker, format, arg);
        }
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.debug(marker, format, arg1, arg2);
        }
    }

    public void debugWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.debug(marker, format, arg1.get(), arg2.get());
        }
    }

    public void debugWithGeneratedArgs(Marker marker, String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.debug(marker, format, arg1, arg2.get());
        }
    }

    public void debugWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.debug(marker, format, arg1.get(), arg2);
        }
    }

    @Override
    public void debug(Marker marker, String format, Object... argArray) {
        if (acquire()) {
            delegate.debug(marker, format, argArray);
        }
    }

    public void debugWithGeneratedArgs(Marker marker, String format, Object... args) {
        if (acquire()) {
            delegate.debug(marker, format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        if (acquire()) {
            delegate.debug(marker, msg, t);
        }
    }

    public void debugWithGeneratedArgs(Marker marker, Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.debug(marker, msg.get(), t.get());
        }
    }

    public void debugWithGeneratedArgs(Marker marker, String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.debug(marker, msg, t.get());
        }
    }

    public void debugWithGeneratedArgs(Marker marker, Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.debug(marker, msg.get(), t);
        }
    }

    @Override
    public void info(String msg) {
        if (acquire()) {
            delegate.info(msg);
        }
    }

    public void infoWithGeneratedArgs(Supplier<String> msg) {
        if (acquire()) {
            delegate.info(msg.get());
        }
    }

    @Override
    public void info(String format, Object arg) {
        if (acquire()) {
            delegate.info(format, arg);
        }
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.info(format, arg1, arg2);
        }
    }

    public void infoWithGeneratedArgs(String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.info(format, arg1.get(), arg2.get());
        }
    }

    public void infoWithGeneratedArgs(String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.info(format, arg1.get(), arg2);
        }
    }

    public void infoWithGeneratedArgs(String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.info(format, arg1, arg2.get());
        }
    }

    @Override
    public void info(String format, Object... arguments) {
        if (acquire()) {
            delegate.info(format, arguments);
        }
    }

    public void infoWithGeneratedArgs(String format, Object... args) {
        if (acquire()) {
            delegate.info(format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void info(String msg, Throwable t) {
        if (acquire()) {
            delegate.info(msg, t);
        }
    }

    public void infoWithGeneratedArgs(Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.info(msg.get(), t);
        }
    }

    public void infoWithGeneratedArgs(Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.info(msg.get(), t.get());
        }
    }

    public void infoWithGeneratedArgs(String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.info(msg, t.get());
        }
    }

    @Override
    public void info(Marker marker, String msg) {
        if (acquire()) {
            delegate.info(marker, msg);
        }
    }

    public void infoWithGeneratedArgs(Marker marker, Supplier<String> msg) {
        if (acquire()) {
            delegate.info(marker, msg.get());
        }
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        if (acquire()) {
            delegate.info(marker, format, arg);
        }
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.info(marker, format, arg1, arg2);
        }
    }

    public void infoWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.info(marker, format, arg1.get(), arg2.get());
        }
    }

    public void infoWithGeneratedArgs(Marker marker, String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.info(marker, format, arg1, arg2.get());
        }
    }

    public void infoWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.info(marker, format, arg1.get(), arg2);
        }
    }

    @Override
    public void info(Marker marker, String format, Object... argArray) {
        if (acquire()) {
            delegate.info(marker, format, argArray);
        }
    }

    public void infoWithGeneratedArgs(Marker marker, String format, Object... args) {
        if (acquire()) {
            delegate.info(marker, format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        if (acquire()) {
            delegate.info(marker, msg, t);
        }
    }

    public void infoWithGeneratedArgs(Marker marker, Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.info(marker, msg.get(), t.get());
        }
    }

    public void infoWithGeneratedArgs(Marker marker, String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.info(marker, msg, t.get());
        }
    }

    public void infoWithGeneratedArgs(Marker marker, Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.info(marker, msg.get(), t);
        }
    }

    @Override
    public void warn(String msg) {
        if (acquire()) {
            delegate.warn(msg);
        }
    }

    public void warnWithGeneratedArgs(Supplier<String> msg) {
        if (acquire()) {
            delegate.warn(msg.get());
        }
    }

    @Override
    public void warn(String format, Object arg) {
        if (acquire()) {
            delegate.warn(format, arg);
        }
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.warn(format, arg1, arg2);
        }
    }

    public void warnWithGeneratedArgs(String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.warn(format, arg1.get(), arg2.get());
        }
    }

    public void warnWithGeneratedArgs(String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.warn(format, arg1.get(), arg2);
        }
    }

    public void warnWithGeneratedArgs(String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.warn(format, arg1, arg2.get());
        }
    }

    @Override
    public void warn(String format, Object... arguments) {
        if (acquire()) {
            delegate.warn(format, arguments);
        }
    }

    public void warnWithGeneratedArgs(String format, Object... args) {
        if (acquire()) {
            delegate.warn(format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void warn(String msg, Throwable t) {
        if (acquire()) {
            delegate.warn(msg, t);
        }
    }

    public void warnWithGeneratedArgs(Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.warn(msg.get(), t);
        }
    }

    public void warnWithGeneratedArgs(Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.warn(msg.get(), t.get());
        }
    }

    public void warnWithGeneratedArgs(String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.warn(msg, t.get());
        }
    }

    @Override
    public void warn(Marker marker, String msg) {
        if (acquire()) {
            delegate.warn(marker, msg);
        }
    }

    public void warnWithGeneratedArgs(Marker marker, Supplier<String> msg) {
        if (acquire()) {
            delegate.warn(marker, msg.get());
        }
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        if (acquire()) {
            delegate.warn(marker, format, arg);
        }
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.warn(marker, format, arg1, arg2);
        }
    }

    public void warnWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.warn(marker, format, arg1.get(), arg2.get());
        }
    }

    public void warnWithGeneratedArgs(Marker marker, String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.warn(marker, format, arg1, arg2.get());
        }
    }

    public void warnWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.warn(marker, format, arg1.get(), arg2);
        }
    }

    @Override
    public void warn(Marker marker, String format, Object... argArray) {
        if (acquire()) {
            delegate.warn(marker, format, argArray);
        }
    }

    public void warnWithGeneratedArgs(Marker marker, String format, Object... args) {
        if (acquire()) {
            delegate.warn(marker, format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        if (acquire()) {
            delegate.warn(marker, msg, t);
        }
    }

    public void warnWithGeneratedArgs(Marker marker, Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.warn(marker, msg.get(), t.get());
        }
    }

    public void warnWithGeneratedArgs(Marker marker, String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.warn(marker, msg, t.get());
        }
    }

    public void warnWithGeneratedArgs(Marker marker, Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.warn(marker, msg.get(), t);
        }
    }

    @Override
    public void error(String msg) {
        if (acquire()) {
            delegate.error(msg);
        }
    }

    public void errorWithGeneratedArgs(Supplier<String> msg) {
        if (acquire()) {
            delegate.error(msg.get());
        }
    }

    @Override
    public void error(String format, Object arg) {
        if (acquire()) {
            delegate.error(format, arg);
        }
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.error(format, arg1, arg2);
        }
    }

    public void errorWithGeneratedArgs(String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.error(format, arg1.get(), arg2.get());
        }
    }

    public void errorWithGeneratedArgs(String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.error(format, arg1.get(), arg2);
        }
    }

    public void errorWithGeneratedArgs(String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.error(format, arg1, arg2.get());
        }
    }

    @Override
    public void error(String format, Object... arguments) {
        if (acquire()) {
            delegate.error(format, arguments);
        }
    }

    public void errorWithGeneratedArgs(String format, Object... args) {
        if (acquire()) {
            delegate.error(format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void error(String msg, Throwable t) {
        if (acquire()) {
            delegate.error(msg, t);
        }
    }

    public void errorWithGeneratedArgs(Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.error(msg.get(), t);
        }
    }

    public void errorWithGeneratedArgs(Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.error(msg.get(), t.get());
        }
    }

    public void errorWithGeneratedArgs(String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.error(msg, t.get());
        }
    }

    @Override
    public void error(Marker marker, String msg) {
        if (acquire()) {
            delegate.error(marker, msg);
        }
    }

    public void errorWithGeneratedArgs(Marker marker, Supplier<String> msg) {
        if (acquire()) {
            delegate.error(marker, msg.get());
        }
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        if (acquire()) {
            delegate.error(marker, format, arg);
        }
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        if (acquire()) {
            delegate.error(marker, format, arg1, arg2);
        }
    }

    public void errorWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.error(marker, format, arg1.get(), arg2.get());
        }
    }

    public void errorWithGeneratedArgs(Marker marker, String format, Object arg1, Supplier<Object> arg2) {
        if (acquire()) {
            delegate.error(marker, format, arg1, arg2.get());
        }
    }

    public void errorWithGeneratedArgs(Marker marker, String format, Supplier<Object> arg1, Object arg2) {
        if (acquire()) {
            delegate.error(marker, format, arg1.get(), arg2);
        }
    }

    @Override
    public void error(Marker marker, String format, Object... argArray) {
        if (acquire()) {
            delegate.error(marker, format, argArray);
        }
    }

    public void errorWithGeneratedArgs(Marker marker, String format, Object... args) {
        if (acquire()) {
            delegate.error(marker, format, calculateGeneratedArgs(args));
        }
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        if (acquire()) {
            delegate.error(marker, msg, t);
        }
    }

    public void errorWithGeneratedArgs(Marker marker, Supplier<String> msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.error(marker, msg.get(), t.get());
        }
    }

    public void errorWithGeneratedArgs(Marker marker, String msg, Supplier<Throwable> t) {
        if (acquire()) {
            delegate.error(marker, msg, t.get());
        }
    }

    public void errorWithGeneratedArgs(Marker marker, Supplier<String> msg, Throwable t) {
        if (acquire()) {
            delegate.error(marker, msg.get(), t);
        }
    }
}
