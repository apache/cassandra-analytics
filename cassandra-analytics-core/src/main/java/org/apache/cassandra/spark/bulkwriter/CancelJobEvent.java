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

package org.apache.cassandra.spark.bulkwriter;

/**
 * A simple data structure to describe an event that leads to job cancellation.
 * It contains the reason of cancellation and optionally the cause
 */
public class CancelJobEvent
{
    public final Throwable exception;
    public final String reason;

    public CancelJobEvent(String reason)
    {
        this.reason = reason;
        this.exception = null;
    }

    public CancelJobEvent(String reason, Throwable throwable)
    {
        this.reason = reason;
        this.exception = throwable;
    }
}
