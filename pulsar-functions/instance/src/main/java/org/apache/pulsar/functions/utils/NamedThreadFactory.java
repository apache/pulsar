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

package org.apache.pulsar.functions.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    /**
     *  threadPool seq
     */
    private static final AtomicInteger SEQ = new AtomicInteger(1);

    /**
     *  thread number
     */
    private final AtomicInteger threadNum = new AtomicInteger(1);


    /**
     *  threadPool prefix
     */
    private final String prefix;

    /**
     *  isDaemon
     */
    private final boolean daemon;

    /**
     *  ThreadGroup
     */
    private final ThreadGroup group;

    public NamedThreadFactory() {
        this("pool-" + SEQ.getAndIncrement(), false);
    }

    /**
     *  Construct the NamedThreadFactory.
     * @param prefix  threadFactory prefix
     */
    public NamedThreadFactory(String prefix) {
        this(prefix, false);
    }


    /**
     *  Construct the NamedThreadFactory.
     * @param prefix
     * @param daemon
     */
    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix + "-thread-";
        this.daemon = daemon;
        SecurityManager s = System.getSecurityManager();
        group = (s == null) ? Thread.currentThread().getThreadGroup() : s.getThreadGroup();
    }

    /**
     *  Construct the thread.
     * @param runnable
     * @return
     */
    public Thread newThread(Runnable runnable) {
        String name = prefix + threadNum.getAndIncrement();
        Thread ret = new Thread(group, runnable, name, 0);
        ret.setDaemon(daemon);
        return ret;
    }

    /**
     *  get ThreadGroup
     * @return
     */
    public ThreadGroup getThreadGroup() {
        return group;
    }


}
