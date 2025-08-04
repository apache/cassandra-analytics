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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FilterUtilsTests
{
    @Test()
    public void testPartialPartitionKeyFilter()
    {
        Filter[] filters = new Filter[]{new EqualTo("a", "1")};
        assertThatThrownBy(() -> FilterUtils.extractPartitionKeyValues(filters, new HashSet<>(Arrays.asList("a", "b"))))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testValidPartitionKeyValuesExtracted()
    {
        Filter[] filters = new Filter[]{new EqualTo("a", "1"), new In("b", new String[]{"2", "3"}), new EqualTo("c", "2")};
        Map<String, List<String>> partitionKeyValues = FilterUtils.extractPartitionKeyValues(filters, new HashSet<>(Arrays.asList("a", "b")));
        assertThat(partitionKeyValues).containsKey("c");
        assertThat(partitionKeyValues).containsKey("a");
        assertThat(partitionKeyValues).containsKey("b");
    }

    @Test()
    public void testCartesianProductOfInValidValues()
    {
        List<List<String>> orderedValues = Arrays.asList(Arrays.asList("1", "2"), Arrays.asList("a", "b", "c"), Collections.emptyList());
        assertThatThrownBy(() -> FilterUtils.cartesianProduct(orderedValues))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCartesianProductOfEmptyList()
    {
        List<List<String>> orderedValues = Collections.emptyList();
        List<List<String>> product = FilterUtils.cartesianProduct(orderedValues);
        assertThat(product).isNotEmpty();
        assertThat(product).hasSize(1);
        assertThat(product.get(0)).isEmpty();
    }

    @Test
    public void testCartesianProductOfSingleton()
    {
        List<List<String>> orderedValues = Collections.singletonList(Arrays.asList("a", "b", "c"));
        assertThat(FilterUtils.cartesianProduct(orderedValues)).hasSize(3);
    }
}
