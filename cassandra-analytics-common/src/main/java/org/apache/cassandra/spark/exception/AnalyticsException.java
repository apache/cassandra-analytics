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
 * Cassandra Analytics exceptions base class for exception handling
 * Note that it is a RuntimeException (unchecked). It gives call-sites the flexibility of handling the exceptions they cared about without forcing the
 * catch blocks (and making the code overly verbose). In most cases, the call-sites simply converts a checked exception into unchecked and rethrow.
 */
public abstract class AnalyticsException extends RuntimeException
{
    private static final long serialVersionUID = 3980444570316598756L;

    public AnalyticsException(String message)
    {
        super(message);
    }

    public AnalyticsException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public AnalyticsException(Throwable cause)
    {
        super(cause);
    }

    protected AnalyticsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
