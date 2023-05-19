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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;

public final class FilterUtils
{
    private FilterUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static Map<String, List<String>> extractPartitionKeyValues(Filter[] filters,
                                                                      Set<String> partitionKeyColumnNames)
    {
        HashMap<String, List<String>> keyValuePairs = new HashMap<>();

        Arrays.stream(filters)
                .filter(filter -> filter instanceof EqualTo || filter instanceof In)
                .forEach(filter -> {
                    final AbstractMap.SimpleEntry<String, List<String>> filterKeyValue = filter instanceof EqualTo
                            ? new AbstractMap.SimpleEntry<>(((EqualTo) filter).attribute(),
                                                            Collections.singletonList(((EqualTo) filter).value().toString()))
                            : new AbstractMap.SimpleEntry<>(((In) filter).attribute(),
                                                            Arrays.stream(((In) filter).values())
                                                                  .map(Object::toString)
                                                                  .collect(Collectors.toList()));

                    Preconditions.checkArgument(!keyValuePairs.containsKey(filterKeyValue.getKey()),
                                                "Duplicate filter passed for key " + filterKeyValue.getKey());
                    if (partitionKeyColumnNames.contains(filterKeyValue.getKey()))
                    {
                        keyValuePairs.put(filterKeyValue.getKey(), filterKeyValue.getValue());
                    }
                });

        if (keyValuePairs.size() == 0)
        {
            return Collections.emptyMap();
        }

        Preconditions.checkArgument(keyValuePairs.keySet().containsAll(partitionKeyColumnNames),
                                    "Invalid filter, all partition key parts must be restricted by = or in");
        return keyValuePairs;
    }

    public static List<List<String>> cartesianProduct(List<List<String>> orderedValues)
    {
        List<List<String>> combinations = new ArrayList<>();

        Preconditions.checkArgument(orderedValues.stream().noneMatch(List::isEmpty));
        int sizeOfProduct = orderedValues.size();
        int[] indices = new int[sizeOfProduct];

        while (true)
        {
            List<String> currProduct = new ArrayList<>();
            for (int index = 0; index < sizeOfProduct; index++)
            {
                currProduct.add(orderedValues.get(index).get(indices[index]));
            }
            combinations.add(currProduct);

            int position = 0;
            while (position < sizeOfProduct && indices[position] + 1 >= orderedValues.get(position).size())
            {
                position++;
            }

            if (position == sizeOfProduct)
            {
                return combinations;
            }

            indices[position]++;
            for (int index = position - 1; index >= 0; index--)
            {
                indices[index] = 0;
            }
        }
    }
}
