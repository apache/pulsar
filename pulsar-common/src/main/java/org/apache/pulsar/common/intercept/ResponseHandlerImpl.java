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
package org.apache.pulsar.common.intercept;

import io.netty.util.Recycler;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * Default implementation of ResponseHandler.
 */
@Slf4j
public class ResponseHandlerImpl implements ResponseHandler {

    private final List<ResponseListener> listeners = new ArrayList<>();

    /**
     * Called by the broker before send response to the client.
     */
    public void complete(PulsarApi.BaseCommand response) {
        for (ResponseListener listener : listeners) {
            try {
                listener.onResponse(response, listener.getContext());
            } catch (Exception e) {
                log.error("Failed on the response listener.", e);
            }
        }
    }

    /**
     * Called by the interceptor want to capture the response.
     */
    public void onResponse(ResponseListener listener) {
        listeners.add(listener);
    }

    private final Recycler.Handle<ResponseHandlerImpl> recyclerHandle;

    private static final Recycler<ResponseHandlerImpl> RECYCLER = new Recycler<ResponseHandlerImpl>() {
        @Override
        protected ResponseHandlerImpl newObject(Recycler.Handle<ResponseHandlerImpl> recyclerHandle) {
            return new ResponseHandlerImpl(recyclerHandle);
        }
    };

    private ResponseHandlerImpl(Recycler.Handle<ResponseHandlerImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static ResponseHandlerImpl create() {
        return RECYCLER.get();
    }

    @Override
    public void recycle() {
        this.listeners.clear();
        recyclerHandle.recycle(this);
    }
}
