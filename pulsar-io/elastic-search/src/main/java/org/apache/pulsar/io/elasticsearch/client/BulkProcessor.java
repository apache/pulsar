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
package org.apache.pulsar.io.elasticsearch.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import org.apache.pulsar.functions.api.Record;

/**
 * Processor for "bulk" call to the Elastic REST Endpoint.
 */
public interface BulkProcessor extends Closeable {

    @Builder
    @Getter
    class BulkOperationRequest {
        private Record pulsarRecord;
    }

    @Builder
    @Getter
    class BulkOperationResult {
        private String error;
        private String index;
        private String documentId;
        public boolean isError() {
            return error != null;
        }
    }

    interface Listener {

        void afterBulk(long executionId, List<BulkOperationRequest> bulkOperationList,
                       List<BulkOperationResult> results);

        void afterBulk(long executionId, List<BulkOperationRequest> bulkOperationList, Throwable throwable);
    }

    @Builder
    @Getter
    class BulkIndexRequest {
        private Record record;
        private long requestId;
        private String index;
        private String documentId;
        private String documentSource;
    }

    @Builder
    @Getter
    class BulkDeleteRequest {
        private Record record;
        private long requestId;
        private String index;
        private String documentId;
    }


    void appendIndexRequest(BulkIndexRequest request) throws IOException;

    void appendDeleteRequest(BulkDeleteRequest request) throws IOException;

    void flush();

    @Override
    void close();
}
