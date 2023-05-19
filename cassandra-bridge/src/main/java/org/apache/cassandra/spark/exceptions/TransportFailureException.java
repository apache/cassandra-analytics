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

package org.apache.cassandra.spark.exceptions;

import org.jetbrains.annotations.Nullable;

public class TransportFailureException extends RuntimeException
{
    public static final int NOT_FOUND_404 = 404;

    @Nullable
    protected final transient Integer statusCode;

    public static TransportFailureException of(String message, Throwable cause)
    {
        return new TransportFailureException(null, message, cause);
    }

    public static TransportFailureException nonretryable(int statusCode)
    {
        return new Nonretryable(statusCode);
    }

    public static TransportFailureException retryExhausted(String message, Throwable cause)
    {
        return new RetryExhausted(message, cause);
    }

    public static TransportFailureException unexpectedResponseType(int statusCode)
    {
        return new UnexpectedResponseType(statusCode);
    }

    public boolean isNotFound()
    {
        return statusCode != null && statusCode == NOT_FOUND_404;
    }

    public TransportFailureException(int statusCode, String message)
    {
        super(message);
        this.statusCode = statusCode;
    }

    public TransportFailureException(@Nullable Integer statusCode, String message, Throwable cause)
    {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public TransportFailureException(@Nullable Integer statusCode, Throwable cause)
    {
        super(cause);
        this.statusCode = statusCode;
    }

    public static class Nonretryable extends TransportFailureException
    {
        public Nonretryable(int statusCode)
        {
            super(statusCode, "Non-retryable status code: " + statusCode);
        }
    }

    public static class UnexpectedResponseType extends TransportFailureException
    {
        public UnexpectedResponseType(int statusCode)
        {
            super(statusCode, "Unexpected http response type: " + statusCode);
        }
    }

    public static class RetryExhausted extends TransportFailureException
    {
        public RetryExhausted(String message, Throwable cause)
        {
            super(null, message, cause);
        }
    }
}
