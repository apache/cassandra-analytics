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

package org.apache.cassandra.spark.reader.common;

import java.io.IOException;

import org.jetbrains.annotations.Nullable;

/**
 * An unchecked wrapper around IOException
 */
public class SSTableStreamException extends RuntimeException
{
    public SSTableStreamException(String message)
    {
        this(new IOException(message));
    }

    public SSTableStreamException(IOException exception)
    {
        super(exception);
    }

    public IOException getIOException()
    {
        return (IOException) getCause();
    }

    @Nullable
    public static IOException getIOException(@Nullable Throwable throwable)
    {
        if (throwable == null)
        {
            return null;
        }
        if (throwable instanceof SSTableStreamException)
        {
            return ((SSTableStreamException) throwable).getIOException();
        }

        return getIOException(throwable.getCause());
    }
}
