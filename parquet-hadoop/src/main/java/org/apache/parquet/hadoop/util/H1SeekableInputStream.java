/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import java.io.IOException;

/**
 * SeekableInputStream implementation that implements read(ByteBuffer) for
 * Hadoop 1 FSDataInputStream.
 */
class H1SeekableInputStream extends DelegatingSeekableInputStream
  implements IOStatisticsSource {

  private static final Logger LOG =
    LoggerFactory.getLogger(H1SeekableInputStream.class);

  private final FSDataInputStream stream;

  public H1SeekableInputStream(FSDataInputStream stream) {
    super(stream);
    this.stream = stream;
  }

  @Override
  public long getPos() throws IOException {
    return stream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    stream.seek(newPos);
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    stream.readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    stream.readFully(stream.getPos(), bytes, start, len);
  }

  @Override
  public void close() throws IOException {
    HadoopStatistics.logIOStatistics(LOG, stream);
    super.close();
  }

  /**
   * This string value will include statistics from the wrapped stream.
   *
   * @return a string.
   */
  @Override
  public String toString() {
    return super.toString() + " " + stream;
  }

  /**
   * Return any IOStatistics provided by the underlying stream.
   * @return IO stats from the inner stream.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return IOStatisticsSupport.retrieveIOStatistics(stream);
  }
}
