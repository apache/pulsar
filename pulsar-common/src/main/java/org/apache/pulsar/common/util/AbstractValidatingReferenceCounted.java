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
package org.apache.pulsar.common.util;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.Recycler;

public abstract class AbstractValidatingReferenceCounted extends AbstractReferenceCounted {
    private static final boolean refCountCheckOnAccess =
            Boolean.parseBoolean(System.getProperty("pulsar.refcount.check.on_access", "true"));

    /**
     * Validate that the instance hasn't been released before accessing fields.
     * This is a sanity check to ensure that we don't read fields from deallocated objects.
     */
    protected void checkRefCount() {
        if (refCountCheckOnAccess && refCnt() < 1) {
            throw new IllegalReferenceCountException(
                    "Possible double release bug (refCnt=" + refCnt() + "). " + getClass().getSimpleName()
                            + " has been deallocated. ");
        }
    }

    public final void resetRefCnt() {
        setRefCnt(1);
    }

    public static <T extends AbstractValidatingReferenceCounted> T getAndCheck(Recycler<T> recycler) {
        T object = recycler.get();
        object.resetRefCnt();
        return object;
    }
}
