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

package org.apache.cassandra.spark.common.stats;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link JobStatsPublisher} that is used to publish job statistics during the
 * Spark job execution. This implementation logs the stats when published.
 */
public class LogStatsPublisher implements JobStatsPublisher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LogStatsPublisher.class);

    @Override
    public void publish(Map<String, String> stats)
    {
        LOGGER.info("Job Stats: {}", stats);
    }
}
