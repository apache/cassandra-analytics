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

package org.apache.cassandra.spark.exception;

import java.time.Instant;

/**
 * Exception for the case when the time drift between local (spark) and remote (cassandra) is too large
 */
public class TimeSkewTooLargeException extends AnalyticsException
{
    private static final long serialVersionUID = -6748770894292325624L;

    public TimeSkewTooLargeException(int allowableDurationMinutes, Instant localNow, Instant remoteNow, String clusterId)
    {
        super(makeExceptionMessage("Time skew between Spark and Cassandra is too large", allowableDurationMinutes, localNow, remoteNow, clusterId));
    }

    private static String makeExceptionMessage(String summary, int allowableDurationMinutes, Instant localNow, Instant remoteNow, String clusterId)
    {
        return String.format("%s. allowableSkewInMinutes=%d, localTime=%s, remoteCassandraTime=%s, clusterId=%s",
                             summary, allowableDurationMinutes, localNow, remoteNow, clusterId);
    }
}
