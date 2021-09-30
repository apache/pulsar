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
package org.apache.pulsar.common.policies.data.impl;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.RawBookieInfo;

/**
 * Raw bookies information.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class BookiesClusterInfoImpl implements BookiesClusterInfo {

    private List<RawBookieInfo> bookies;

    public static BookiesClusterInfoImplBuilder builder() {
        return new BookiesClusterInfoImplBuilder();
    }

    public static class BookiesClusterInfoImplBuilder implements BookiesClusterInfo.Builder{
        private List<RawBookieInfo> bookies;

        public BookiesClusterInfoImplBuilder bookies(List<RawBookieInfo> bookies) {
            this.bookies = bookies;
            return this;
        }

        public BookiesClusterInfoImpl build() {
            return new BookiesClusterInfoImpl(bookies);
        }
    }
}
