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
package org.apache.pulsar.metadata.impl.batching;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;

@Data
@AllArgsConstructor
public class OpPut implements MetadataOp {
    private final String path;
    private final byte[] data;
    private final Optional<Long> optExpectedVersion;
    private final EnumSet<CreateOption> options;

    private final CompletableFuture<Stat> future = new CompletableFuture<>();

    public boolean isEphemeral() {
        return options.contains(CreateOption.Ephemeral);
    }

    @Override
    public Type getType() {
        return Type.PUT;
    }

    @Override
    public int size() {
        return path.length() + (data != null ? data.length : 0);
    }
}
