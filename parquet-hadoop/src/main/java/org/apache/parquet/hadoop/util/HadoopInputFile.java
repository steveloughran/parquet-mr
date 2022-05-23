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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

public class HadoopInputFile implements InputFile {

  private final FileSystem fs;
  private final FileStatus stat;
  private final Configuration conf;
  private final Path path;
  private final long length;

  public static HadoopInputFile fromPath(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopInputFile(fs, fs.getFileStatus(path), conf);
  }

  public static HadoopInputFile fromPathWithLength(Path path,
    Configuration conf, long length) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopInputFile(fs, conf,  path, length);
  }

  public static HadoopInputFile fromStatus(FileStatus stat, Configuration conf)
      throws IOException {
    FileSystem fs = stat.getPath().getFileSystem(conf);
    return new HadoopInputFile(fs, stat, conf);
  }

  public static HadoopInputFile fromStatus(FileSystem fs, FileStatus stat,
    Configuration conf) {
    return new HadoopInputFile(fs, stat, conf);
  }

  private HadoopInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
    this.fs = fs;
    this.stat = stat;
    this.path = stat.getPath();
    this.length = stat.getLen();
    this.conf = conf;
  }

  public HadoopInputFile(FileSystem fs,
    Configuration conf,
    Path path,
    long length) {
    this.fs = fs;
    this.conf = conf;
    this.path = path;
    this.length = length;
    this.stat = null;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public Path getPath() {
    return path;
  }

  @Override
  public long getLength() {
    return length;
  }

  /**
   * Open a stream using the openFile API and requesting random IO.
   * Also passing in the length if known.
   * {@inheritDoc}.
   */
  @Override
  public SeekableInputStream newStream() throws IOException {
    FutureDataInputStreamBuilder builder = fs.openFile(getPath())
      .opt("fs.s3a.experimental.input.fadvise", "random")
      .opt("fs.s3a.readahead.range", 1024 * 1024)
      .opt("fs.option.openfile.read.policy", "random");

    // convert to a string so that all hadoop releases with the openfile
    // API will linl
    if (length > 0) {
      builder.opt("fs.option.openfile.length",
        Long.toString(length));
    }
    // open the stream, which may be asynchronous.
    CompletableFuture<FSDataInputStream> streamF = builder.build();
    return HadoopStreams.wrap(awaitFuture(streamF));
  }

  @Override
  public String toString() {
    return getPath().toString();
  }
}
