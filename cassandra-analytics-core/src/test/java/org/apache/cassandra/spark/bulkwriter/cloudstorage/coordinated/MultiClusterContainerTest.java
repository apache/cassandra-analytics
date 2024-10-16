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

import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiClusterContainerTest
{
    @Test
    void testReadAndWriteForSingleTarget()
    {
        MultiClusterContainer<Value> container = new MultiClusterContainer<>();
        assertThat(container.getValueOrNull(null)).isNull();
        assertThat(container.getValueOrNull("cluster1")).isNull();

        container.setValue(null, new Value());
        assertThat(container.getValueOrNull(null)).isNotNull();
        assertThat(container.getValueOrNull("cluster1")).isNull();
    }

    @Test
    void testReadAndWriteForMultiClusters()
    {
        MultiClusterContainer<Value> container = new MultiClusterContainer<>();
        container.addAll(ImmutableMap.of("cluster1", new Value()));

        assertThat(container.getValueOrNull("cluster1")).isNotNull();
        assertThat(container.getValueOrNull("cluster2")).isNull();

        container.setValue("cluster2", new Value());
        assertThat(container.getValueOrNull("cluster2")).isNotNull();
    }

    @Test
    void testSetValueFailsDueToConflict()
    {
        MultiClusterContainer<Value> singleContainer = new MultiClusterContainer<>();
        singleContainer.setValue(null, new Value());

        assertThatThrownBy(() -> singleContainer.setValue("cluster1", new Value()))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set value for non-null cluster when the container is used for non-coordinated-write");

        assertThatThrownBy(() -> singleContainer.addAll(ImmutableMap.of("cluster1", new Value())))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set value for non-null cluster when the container is used for non-coordinated-write");

        MultiClusterContainer<Value> multiClusterContainer = new MultiClusterContainer<>();
        multiClusterContainer.addAll(ImmutableMap.of("cluster1", new Value()));

        assertThatThrownBy(() -> multiClusterContainer.setValue(null, new Value()))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set value for null cluster when the container is used for coordinated-write");
    }

    @Test
    void testUpdateValue()
    {
        for (String clusterId : Arrays.asList(null, "cluster1"))
        {
            MultiClusterContainer<Value> container = new MultiClusterContainer<>();
            container.updateValue(clusterId, v -> {
                assertThat(v).isNull();
                return null;
            });

            container.setValue(clusterId, new Value());
            assertThat(container.getValueOrThrow(clusterId).a).isEqualTo(1);
            container.updateValue(clusterId, v -> {
                v.a = 100;
                return v;
            });
            assertThat(container.getValueOrThrow(clusterId).a).isEqualTo(100);

            // remove the value
            container.updateValue(clusterId, v -> null);
            assertThat(container.getValueOrNull(clusterId)).isNull();
        }
    }

    @Test
    void testHashCodeAndEquals()
    {
        Value value = new Value();
        MultiClusterContainer<Value> container1 = new MultiClusterContainer<>();
        container1.setValue(null, value);
        MultiClusterContainer<Value> container2 = new MultiClusterContainer<>();
        container2.setValue(null, value);
        assertThat(container1.hashCode()).isEqualTo(container2.hashCode());
        assertThat(container1).isEqualTo(container2);

        container1 = new MultiClusterContainer<>();
        container1.setValue("cluster1", value);
        container2 = new MultiClusterContainer<>();
        container2.setValue("cluster1", value);
        assertThat(container1.hashCode()).isEqualTo(container2.hashCode());
        assertThat(container1).isEqualTo(container2);

        container1.setValue("cluster2", value);
        assertThat(container1.hashCode()).isNotEqualTo(container2.hashCode());
        assertThat(container1).isNotEqualTo(container2);
    }

    @Test
    void testGetAnyValue()
    {
        for (String clusterId : Arrays.asList(null, "cluster1"))
        {
            MultiClusterContainer<Value> container = new MultiClusterContainer<>();
            assertThat(container.getAnyValue()).isNull();

            container.setValue(clusterId, new Value());
            assertThat(container.getAnyValue()).isNotNull();
        }
    }

    @Test
    void testSize()
    {
        MultiClusterContainer<Value> container = new MultiClusterContainer<>();
        assertThat(container.size()).isZero();

        container.setValue("cluster1", new Value());
        assertThat(container.size()).isOne();

        container.setValue("cluster2", new Value());
        assertThat(container.size()).isEqualTo(2);

        container = new MultiClusterContainer<>();
        container.setValue(null, new Value());
        assertThat(container.size()).isOne();
    }

    private static class Value
    {
        int a = 1;
    }
}
