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
package org.apache.pulsar.functions.worker.dlog;

import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.exceptions.EndOfStreamException;

import java.io.IOException;
import java.io.InputStream;

/**
 * DistributedLog Input Stream.
 */
public class DLInputStream extends InputStream {

  private LogRecordWithInputStream currentLogRecord = null;
  private final DistributedLogManager dlm;
  private LogReader reader;
  private boolean eos = false;

  // Cache the input stream for a log record.
  private static class LogRecordWithInputStream {
    private final InputStream payloadStream;
    private final LogRecordWithDLSN logRecord;

    LogRecordWithInputStream(LogRecordWithDLSN logRecord) {
      this.logRecord = logRecord;
      this.payloadStream = logRecord.getPayLoadInputStream();
    }

    InputStream getPayLoadInputStream() {
      return payloadStream;
    }

    LogRecordWithDLSN getLogRecord() {
      return logRecord;
    }

    // The last txid of the log record is the position of the next byte in the stream.
    // Subtract length to get starting offset.
    long getOffset() {
      return logRecord.getTransactionId() - logRecord.getPayload().length;
    }
  }

  /**
   * Construct DistributedLog input stream.
   *
   * @param dlm the Distributed Log Manager to access the stream
   */
  public DLInputStream(DistributedLogManager dlm) throws IOException {
    this.dlm = dlm;
    reader = dlm.getInputStream(DLSN.InitialDLSN);
  }

  /**
   * Get input stream representing next entry in the
   * ledger.
   *
   * @return input stream, or null if no more entries
   */
  private LogRecordWithInputStream nextLogRecord() throws IOException {
    try {
      return nextLogRecord(reader);
    } catch (EndOfStreamException e) {
      eos = true;
      return null;
    }
  }

  private static LogRecordWithInputStream nextLogRecord(LogReader reader) throws IOException {
    LogRecordWithDLSN record = reader.readNext(false);

    if (null != record) {
      return new LogRecordWithInputStream(record);
    } else {
      record = reader.readNext(false);
      if (null != record) {
        return new LogRecordWithInputStream(record);
      } else {
        return null;
      }
    }
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
    if (eos) {
      return -1;
    }

    int read = 0;
    if (currentLogRecord == null) {
      currentLogRecord = nextLogRecord();
      if (currentLogRecord == null) {
        return read;
      }
    }

    while (read < len) {
      int thisread = currentLogRecord.getPayLoadInputStream().read(b, off + read, len - read);
      if (thisread == -1) {
        currentLogRecord = nextLogRecord();
        if (currentLogRecord == null) {
          return read;
        }
      } else {
        read += thisread;
      }
    }
    return read;
  }

  @Override
  public void close() throws IOException {
    reader.close();
    dlm.close();
  }
}
