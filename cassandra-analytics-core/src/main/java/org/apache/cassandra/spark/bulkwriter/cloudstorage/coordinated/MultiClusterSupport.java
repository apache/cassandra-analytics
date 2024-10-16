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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.util.NoSuchElementException;
import java.util.function.BiConsumer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Support storing values per cluster, iteration and lookup
 * @param <T> value type
 */
public interface MultiClusterSupport<T>
{
    /**
     * @return count of all clusters
     */
    int size();

    /**
     * Iterate through all values
     * @param action function to consume the values
     */
    void forEach(BiConsumer<String, T> action);

    /**
     * Look up a value based on clusterId
     * @param clusterId cluster id
     * @return the value of type T associated with the clusterId, or null if not found
     */
    @Nullable
    T getValueOrNull(@Nullable String clusterId);

    /**
     * Look up a value based on clusterId
     * @param clusterId cluster id
     * @return the value of type T associated with the clusterId, or throws NoSuchElementException
     * @throws NoSuchElementException when no value is found
     */
    @NotNull
    default T getValueOrThrow(@Nullable String clusterId) throws NoSuchElementException
    {
        T v = getValueOrNull(clusterId);
        if (v == null)
        {
            throw new NoSuchElementException("Unable to fetch value from container for cluster: " + clusterId);
        }
        return v;
    }
}
