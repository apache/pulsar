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
package org.apache.pulsar.broker.transaction.buffer;

import com.google.common.annotations.Beta;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.transaction.exception.buffer.TransactionBufferException;


/**
 * A reader to read entries of a given transaction from transaction buffer.
 */
@Beta
public interface TransactionBufferReader extends AutoCloseable {

    /**
     * Read next <tt>numEntries</tt> entries from transaction buffer for the given
     * transaction.
     *
     * <p><tt>numEntries</tt> is the max number of entries to return. The result returned
     * can be less than <tt>numEntries</tt>.
     *
     * @param numEntries the number of entries to read from transaction buffer.
     * @return a future represents the result of the read operations.
     * @throws TransactionBufferException.EndOfTransactionException if reaching end of the transaction and no
     *         more entries to return.
     */
    CompletableFuture<List<TransactionEntry>> readNext(int numEntries);

    /**
     * {@inheritDoc}
     */
    @Override
    void close();
}
