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

import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.api.DistributedLogManager;

import java.io.IOException;
import java.io.OutputStream;

/**
 * DistributedLog Output Stream.
 */
public class DLOutputStream extends OutputStream {

  private final DistributedLogManager dlm;
  private final AppendOnlyStreamWriter writer;

  public DLOutputStream(DistributedLogManager dlm,
                        AppendOnlyStreamWriter writer) {
    this.dlm = dlm;
    this.writer = writer;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] data = new byte[] {
        (byte) b
    };
    write(data);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // TODO: avoid array copy by using the new bytebuf api
    byte[] newData = new byte[len];
    System.arraycopy(b, off, newData, 0, len);
    write(newData);
  }

  @Override
  public void write(byte[] b) throws IOException {
    writer.write(b);
  }

  @Override
  public void flush() throws IOException {
    writer.force(false);
  }

  @Override
  public void close() throws IOException {
    writer.markEndOfStream();
    writer.close();
    dlm.close();
  }
}
