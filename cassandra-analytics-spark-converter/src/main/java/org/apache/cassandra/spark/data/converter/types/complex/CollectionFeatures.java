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

package org.apache.cassandra.spark.data.converter.types.complex;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.data.converter.types.SparkType;

interface CollectionFeatures extends SparkType
{
    default List<SparkType> sparkTypes()
    {
        return collection()
               .types().stream()
               .map(t -> converter().toSparkType(t))
               .collect(Collectors.toList());
    }

    CqlField.CqlCollection collection();

    SparkSqlTypeConverter converter();

    default int size()
    {
        return collection().types().size();
    }

    default SparkType sparkType()
    {
        return sparkType(0);
    }

    default SparkType sparkType(int position)
    {
        return sparkTypes().get(position);
    }
}
