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

package org.apache.cassandra.spark.config;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.sparksql.LastModifiedTimestampDecorator;
import org.apache.cassandra.spark.sparksql.RowBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public enum SchemaFeatureSet implements SchemaFeature
{
    // NOTE: The order matters!

    // Special column that passes over last modified timestamp for a row
    LAST_MODIFIED_TIMESTAMP
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.TimestampType;
            }

            @Override
            public <T extends InternalRow> RowBuilder<T> decorate(RowBuilder<T> builder)
            {
                return new LastModifiedTimestampDecorator<>(builder, fieldName());
            }
        };

    /**
     * Initialize the requested features from the input options
     *
     * @param options
     * @return the requested features list. If none is requested, an empty list is returned
     */
    public static List<SchemaFeature> initializeFromOptions(Map<String, String> options)
    {
        return Arrays.stream(values())
                     .filter(feature -> Boolean.parseBoolean(options.getOrDefault(feature.optionName(), "false").toLowerCase()))
                     .collect(Collectors.toList());
    }
}
