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

import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.parquet.io.DelegatingSeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * SeekableInputStream implementation for FSDataInputStream that implements
 * ByteBufferReadable in Hadoop 2.
 *
 * The wrapped class {@code stream.getWrappedStream()} must implement
 * ByteBufferReadable; this is the fallback if ByteBufferReadable isn't
 * declared as available.
 */
class H2SeekableInputStream extends DelegatingSeekableInputStream
    implements IOStatisticsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(H2SeekableInputStream.class);

  // Visible for testing
  interface Reader {
    int read(ByteBuffer buf) throws IOException;
  }

  private final FSDataInputStream stream;
  private final Reader reader;
  /** Will be false unless the wrapped stream implements the interface. */
  private boolean useByteBufferPositionedReadable;

  public H2SeekableInputStream(FSDataInputStream stream) {
    super(stream);
    this.stream = stream;
    this.reader = new H2Reader();
    useByteBufferPositionedReadable = stream.hasCapability(
        StreamCapabilities.PREADBYTEBUFFER);
  }

  @Override
  public void close() throws IOException {
    HadoopStatistics.logIOStatistics(LOG, stream);
    stream.close();
  }

  @Override
  public long getPos() throws IOException {
    return stream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    stream.seek(newPos);
  }

  /**
   * Return any IOStatistics of the underlying output stream.
   * @return IOStatistics or null.
   */
  @Override
  public IOStatistics getIOStatistics() {
    return stream.getIOStatistics();
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    stream.readFully(bytes, start, len);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return stream.read(buf);
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    if (useByteBufferPositionedReadable) {
      try {
        ((ByteBufferPositionedReadable) (stream)).readFully(
            stream.getPos(), buf);
      } catch (ClassCastException | UnsupportedOperationException e) {
        LOG.warn("Stream {} declared support for ByteBufferPositionedReadable"
            + " yet failed to implement it. Falling back", stream, e);
        useByteBufferPositionedReadable = false;
        // and carry on into the read fully.
        // future invocations will skip the check.
        // two concurrent calls on different threads? they'll both fail and
        // fall back to here.
        readFully(reader, buf);
      }
    } else {
      readFully(reader, buf);
    }
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

  private class H2Reader implements Reader {
    @Override
    public int read(ByteBuffer buf) throws IOException {
      return stream.read(buf);
    }
  }

  public static void readFully(Reader reader, ByteBuffer buf)
      throws IOException {
    // unfortunately the Hadoop APIs seem to not have a 'readFully' equivalent for the byteBuffer read
    // calls. The read(ByteBuffer) call might read fewer than byteBuffer.hasRemaining() bytes. Thus we
    // have to loop to ensure we read them.
    while (buf.hasRemaining()) {
      int readCount = reader.read(buf);
      if (readCount == -1) {
        // this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer
        // that has more remaining than the amount of data in the stream.
        // It can also happen if the connection to the far end closed.
        throw new EOFException(
            "Reached the end of stream. Still have: " + buf.remaining() +
                " bytes left");
      }
    }
  }
}
