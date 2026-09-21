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

/**
 * No replica is available to act as the coordinator of a bulk transfer for one or more token ranges.
 * <p>
 * This applies to tracked (mutation tracking) keyspaces, where the bulk write streams to a single coordinator per
 * range and Cassandra fans the mutations out to the remaining replicas. Such a write does not fail on the job's
 * consistency level up front, only on the absence of any usable coordinator, which is what this exception reports.
 */
public class CoordinatorNotAvailableException extends AnalyticsException
{
    private static final long serialVersionUID = 6394612176398124405L;

    public CoordinatorNotAvailableException(String message)
    {
        super(message);
    }

    public CoordinatorNotAvailableException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
