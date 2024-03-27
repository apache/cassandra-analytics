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

package org.apache.cassandra.spark.common;


import java.util.Objects;

import org.jetbrains.annotations.NotNull;

/**
 * An implementation of {@link Digest} that represents an MD5 digest
 */
public class MD5Digest implements Digest
{
    private final @NotNull String value;

    /**
     * Constructs a new MD5Digest with the provided MD5 {@code value}
     *
     * @param value the MD5 value
     */
    public MD5Digest(@NotNull String value)
    {
        this.value = Objects.requireNonNull(value, "value is required");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String value()
    {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public o.a.c.sidecar.client.shaded.common.data.Digest toSidecarDigest()
    {
        return new o.a.c.sidecar.client.shaded.common.data.MD5Digest(value);
    }

    @Override
    public String toString()
    {
        return "MD5Digest{" +
               "value='" + value + '\'' +
               '}';
    }
}
