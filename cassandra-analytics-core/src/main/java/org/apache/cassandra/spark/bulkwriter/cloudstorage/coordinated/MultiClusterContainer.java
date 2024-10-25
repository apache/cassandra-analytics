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

import java.io.Serializable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A container to hold values per cluster. Each value is identified by its belonging cluster id.
 * It is compatible with single cluster, i.e. no cluster id is defined for the value. Pass null clusterId to get the value.
 * However, the container does not permit holding values for both multi-cluster case and single cluster case;
 * otherwise, {@link IllegalStateException} is thrown.
 * @param <T> value type
 */
public class MultiClusterContainer<T> implements Serializable, MultiClusterSupport<T>
{
    private static final long serialVersionUID = 8387168256773834417L;
    // A special (non-string) key used by non-coordinated write; The actual value (Integer) is serializable constant to ensure ser/deser stability
    private static final Object SENTINEL_KEY = Integer.MAX_VALUE;

    // For coordinated write, the key should be String values of clusterId
    private final Map<Object, T> byCluster = new ConcurrentHashMap<>();

    @Nullable
    @Override
    public T getValueOrNull(@Nullable String clusterId)
    {
        return byCluster.get(key(clusterId));
    }

    /**
     * @return any value in the container. Null is returned if the container is empty.
     */
    @Nullable
    public T getAnyValue()
    {
        T v = null;
        if (!byCluster.isEmpty())
        {
            v = byCluster.values().iterator().next();
        }
        return v;
    }

    /**
     * @return any value in the container
     * @throws NoSuchElementException when no value can be found
     */
    @NotNull
    public T getAnyValueOrThrow() throws NoSuchElementException
    {
        T v = getAnyValue();
        if (v == null)
        {
            throw new NoSuchElementException("No value is found");
        }
        return v;
    }

    @Override
    public int size()
    {
        return byCluster.size();
    }

    @Override
    public void forEach(BiConsumer<String, T> action)
    {
        byCluster.forEach((key, value) -> {
            if (key == SENTINEL_KEY)
            {
                action.accept(null, value);
            }
            else
            {
                action.accept((String) key, value);
            }
        });
    }

    /**
     * Set the value for a cluster
     *
     * @param clusterId nullable cluster id. When the value is null, it reads from the single value
     */
    public void setValue(@Nullable String clusterId, @NotNull T value)
    {
        if (clusterId == null)
        {
            Preconditions.checkState(byCluster.isEmpty() || byCluster.containsKey(SENTINEL_KEY),
                                     "Cannot set value for null cluster when the container is used for coordinated-write");
            byCluster.put(SENTINEL_KEY, value);
        }
        else
        {
            Preconditions.checkState(!byCluster.containsKey(SENTINEL_KEY),
                                     "Cannot set value for non-null cluster when the container is used for non-coordinated-write");
            byCluster.put(clusterId, value);
        }
    }

    /**
     * Update the value associated with the clusterId
     *
     * @param clusterId nullable cluster id. When the value is null, it updates the single value
     * @param valueUpdater function to update the value; if not prior value exist, the updater receives null as input,
     *                     If the updater returns null, the value associated with the cluster is removed
     */
    public void updateValue(@Nullable String clusterId, UnaryOperator<T> valueUpdater)
    {
        if (clusterId == null)
        {
            Preconditions.checkState(byCluster.isEmpty() || byCluster.containsKey(SENTINEL_KEY),
                                     "Cannot set value for null cluster when the container is used for coordinated-write");
            byCluster.compute(SENTINEL_KEY, (id, value) -> valueUpdater.apply(value));
        }
        else
        {
            Preconditions.checkState(!byCluster.containsKey(SENTINEL_KEY),
                                     "Cannot set value for non-null cluster when the container is used for non-coordinated-write");
            byCluster.compute(clusterId, (id, value) -> valueUpdater.apply(value));
        }
    }

    /**
     * Add all values from the map
     * @param clusters map of value per cluster
     */
    public void addAll(Map<String, T> clusters)
    {
        Preconditions.checkState(!byCluster.containsKey(SENTINEL_KEY),
                                 "Cannot set value for non-null cluster when the container is used for non-coordinated-write");
        byCluster.putAll(clusters);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (!(obj instanceof MultiClusterContainer))
        {
            return false;
        }

        MultiClusterContainer<?> that = (MultiClusterContainer<?>) obj;
        return Objects.equals(byCluster, that.byCluster);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(byCluster);
    }

    private Object key(@Nullable String clusterId)
    {
        return clusterId == null ? SENTINEL_KEY : clusterId;
    }
}
