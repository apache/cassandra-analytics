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
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of {@link Digest} that represents an XXHash32 digest
 */
public class XXHash32Digest implements Digest
{
    private final @NotNull String value;
    private final @NotNull String seedHex;

    /**
     * Constructs a new XXHashDigest with the provided XXHash {@code value} and the seed value represented as
     * a hexadecimal string
     *
     * @param value the xxhash value
     * @param seed  the value of the seed used to calculate the digest
     */
    public XXHash32Digest(@NotNull String value, int seed)
    {
        this.value = Objects.requireNonNull(value, "value is required");
        this.seedHex = Integer.toHexString(seed);
    }

    /**
     * @return the string representation of the digest
     */
    @Override
    public String value()
    {
        return value;
    }

    /**
     * @return the optional seed in hexadecimal format
     */
    public @Nullable String seedHex()
    {
        return seedHex;
    }

    @Override
    public o.a.c.sidecar.client.shaded.common.request.data.Digest toSidecarDigest()
    {
        return new o.a.c.sidecar.client.shaded.common.request.data.XXHash32Digest(value, seedHex);
    }

    @Override
    public String toString()
    {
        return "XXHash32Digest{" +
               "value='" + value + '\'' +
               ", seedHex='" + seedHex + '\'' +
               '}';
    }
}
