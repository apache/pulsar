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
package org.apache.pulsar.functions.worker.dlog;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.EndOfStreamException;

/**
 * DistributedLog Input Stream.
 *
 * <p>Reads the records of a log stream until the end-of-stream marker written by {@link DLOutputStream#close()}.
 * A log stream is a tailing log: a reader of a stream that has no end-of-stream marker (for example because the
 * writer failed before completing the upload, or the stream was never written to) waits for more records to arrive
 * forever. To avoid blocking the calling thread indefinitely, waiting for the next records is bounded by a read
 * timeout and fails with an {@link IOException} when it expires.
 */
public class DLInputStream extends InputStream {

  /**
   * Default maximum time to wait for the next records of the log stream to become available.
   */
  public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(60);

  private static final int READ_BATCH_SIZE = 100;

  private final DistributedLogManager dlm;
  private final AsyncLogReader reader;
  private final long readTimeoutMs;
  private final Deque<LogRecordWithDLSN> pendingRecords = new ArrayDeque<>();
  // payload of the log record that is currently being consumed
  private InputStream currentPayload;
  private boolean eos = false;

  /**
   * Construct DistributedLog input stream.
   *
   * @param dlm the Distributed Log Manager to access the stream
   */
  public DLInputStream(DistributedLogManager dlm) throws IOException {
    this(dlm, DEFAULT_READ_TIMEOUT);
  }

  /**
   * Construct DistributedLog input stream.
   *
   * @param dlm the Distributed Log Manager to access the stream
   * @param readTimeout maximum time to wait for the next records of the stream to become available
   */
  public DLInputStream(DistributedLogManager dlm, Duration readTimeout) throws IOException {
    this.dlm = dlm;
    this.readTimeoutMs = readTimeout.toMillis();
    this.reader = await(dlm.openAsyncLogReader(DLSN.InitialDLSN), "open a reader for");
  }

  private <T> T await(CompletableFuture<T> future, String operation) throws IOException {
    try {
      return future.get(readTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      future.cancel(false);
      throw new IOException("Timed out after " + readTimeoutMs + " ms waiting to " + operation + " log stream "
          + dlm.getStreamName(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while waiting to " + operation + " log stream "
          + dlm.getStreamName());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Failed to " + operation + " log stream " + dlm.getStreamName(), cause);
    }
  }

  /**
   * Get the next log record of the stream.
   *
   * @return the next log record, or null when the end of the stream has been reached
   */
  private LogRecordWithDLSN nextLogRecord() throws IOException {
    if (pendingRecords.isEmpty() && !eos) {
      try {
        List<LogRecordWithDLSN> records = await(reader.readBulk(READ_BATCH_SIZE), "read");
        pendingRecords.addAll(records);
      } catch (EndOfStreamException e) {
        eos = true;
      }
    }
    return pendingRecords.pollFirst();
  }

  /**
   * Get the payload of the log record that is currently being consumed, moving on to the next log record when the
   * current payload has been fully consumed.
   *
   * @return the payload input stream, or null when the end of the stream has been reached
   */
  private InputStream currentPayload() throws IOException {
    if (currentPayload == null) {
      LogRecordWithDLSN record = nextLogRecord();
      if (record == null) {
        return null;
      }
      currentPayload = record.getPayLoadInputStream();
    }
    return currentPayload;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    if (read(b, 0, 1) != 1) {
      return -1;
    } else {
      return b[0];
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.checkFromIndexSize(off, len, b.length);
    if (len == 0) {
      return 0;
    }
    int read = 0;
    while (read < len) {
      InputStream payload = currentPayload();
      if (payload == null) {
        break;
      }
      int thisread = payload.read(b, off + read, len - read);
      if (thisread == -1) {
        currentPayload = null;
      } else {
        read += thisread;
      }
    }
    return read == 0 ? -1 : read;
  }

  @Override
  public void close() throws IOException {
    try {
      await(reader.asyncClose(), "close the reader of");
    } finally {
      dlm.close();
    }
  }
}
