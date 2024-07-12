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

package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.exceptions.TransportFailureException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThrowableUtilTests
{
    @Test
    public void testNoNesting()
    {
        Throwable throwable = new RuntimeException();
        assertEquals(throwable, ThrowableUtils.rootCause(throwable));
    }

    @Test
    public void testNested()
    {
        Throwable throwable2 = new RuntimeException();
        Throwable throwable1 = new RuntimeException(throwable2);
        assertEquals(throwable2, ThrowableUtils.rootCause(throwable1));
        assertEquals(throwable2, ThrowableUtils.rootCause(throwable2));
    }

    @Test
    public void testMultiNested()
    {
        Throwable throwable4 = new RuntimeException();
        Throwable throwable3 = new RuntimeException(throwable4);
        Throwable throwable2 = new RuntimeException(throwable3);
        Throwable throwable1 = new RuntimeException(throwable2);
        assertEquals(throwable4, ThrowableUtils.rootCause(throwable1));
        assertEquals(throwable4, ThrowableUtils.rootCause(throwable2));
        assertEquals(throwable4, ThrowableUtils.rootCause(throwable3));
        assertEquals(throwable4, ThrowableUtils.rootCause(throwable4));
    }

    @Test
    public void testOfType()
    {
        IOException io = new IOException();
        Throwable throwable = new RuntimeException(io);
        assertEquals(io, ThrowableUtils.rootCause(throwable, IOException.class));
        assertEquals(io, ThrowableUtils.rootCause(io, IOException.class));
    }

    @Test
    public void testOfType2()
    {
        TransportFailureException exception = TransportFailureException.nonretryable(404);
        Throwable throwable = new RuntimeException(exception);
        assertEquals(exception, ThrowableUtils.rootCause(throwable, TransportFailureException.class));
        assertTrue(Objects.requireNonNull(ThrowableUtils.rootCause(throwable, TransportFailureException.class)).isNotFound());
        assertEquals(exception, ThrowableUtils.rootCause(exception, TransportFailureException.class));
    }

    @Test
    public void testOfTypeNested()
    {
        Throwable throwable4 = new RuntimeException();
        IOException io = new IOException(throwable4);
        Throwable throwable3 = new RuntimeException(io);
        Throwable throwable2 = new RuntimeException(throwable3);
        Throwable throwable1 = new RuntimeException(throwable2);
        assertEquals(io, ThrowableUtils.rootCause(throwable1, IOException.class));
        assertEquals(io, ThrowableUtils.rootCause(throwable2, IOException.class));
        assertNull(ThrowableUtils.rootCause(throwable4, IOException.class));
    }

    @Test
    public void testOfTypeNotFound()
    {
        Throwable throwable = new RuntimeException();
        assertNull(ThrowableUtils.rootCause(throwable, IOException.class));
    }

    @Test
    public void testOfTypeNotExist()
    {
        Throwable throwable4 = new RuntimeException();
        Throwable throwable3 = new RuntimeException(throwable4);
        Throwable throwable2 = new RuntimeException(throwable3);
        Throwable throwable1 = new RuntimeException(throwable2);
        assertNull(ThrowableUtils.rootCause(throwable1, IOException.class));
    }
}
