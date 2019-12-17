/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Binding to Hadoop IOStatistics..
 */
public class HadoopStatistics {
  private static final Logger LOG =
    LoggerFactory.getLogger(HadoopStatistics.class);

  public static void logIOStatistics(Object source) {
    logIOStatistics(LOG, source);
  }
  public static void logIOStatistics(Logger log, Object source) {
    if (source instanceof IOStatisticsSource) {
      log.info("statistics {}",
        IOStatisticsLogging.demandStringifyIOStatisticsSource(
          (IOStatisticsSource) source));
    } else if (source instanceof IOStatistics) {
      log.info("statistics {}",
        IOStatisticsLogging.demandStringifyIOStatistics(
          (IOStatistics) source));
    }
  }
}
