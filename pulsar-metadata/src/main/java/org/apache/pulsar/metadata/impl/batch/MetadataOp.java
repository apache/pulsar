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

package org.apache.pulsar.metadata.impl.batch;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.zookeeper.Op;

@Data
@AllArgsConstructor
public abstract class MetadataOp<T> {

    public static final int TYPE_GET_DATA = 1;
    public static final int TYPE_GET_CHILDREN = 2;


    final int type;
    final String path;
    final CompletableFuture<T> future;
    public abstract Op toZkOp();

    public static class OpGetData extends MetadataOp<Optional<GetResult>> {

        OpGetData(String path) {
            super(TYPE_GET_DATA, path, new CompletableFuture<>());
        }

        @Override
        public Op toZkOp() {
            return Op.getData(path);
        }
    }

    public static class OpGetChildren extends MetadataOp<List<String>> {

        OpGetChildren(String path) {
            super(TYPE_GET_CHILDREN, path, new CompletableFuture<>());
        }

        @Override
        public Op toZkOp() {
            return Op.getChildren(path);
        }
    }

    public static OpGetData getData(String path) {
        return new OpGetData(path);
    }

    public static OpGetChildren getChildren(String path) {
        return new OpGetChildren(path);
    }
}
