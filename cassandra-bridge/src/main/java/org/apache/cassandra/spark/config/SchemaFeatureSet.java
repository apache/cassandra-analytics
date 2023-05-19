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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.sparksql.CellTombstonesInComplexDecorator;
import org.apache.cassandra.spark.sparksql.LastModifiedTimestampDecorator;
import org.apache.cassandra.spark.sparksql.RangeTombstoneDecorator;
import org.apache.cassandra.spark.sparksql.RangeTombstoneMarker;
import org.apache.cassandra.spark.sparksql.RowBuilder;
import org.apache.cassandra.spark.sparksql.UpdateFlagDecorator;
import org.apache.cassandra.spark.sparksql.UpdatedFieldsIndicatorDecorator;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
            public RowBuilder decorate(RowBuilder builder)
            {
                return new LastModifiedTimestampDecorator(builder, fieldName());
            }
        },

    // Special column that passes over updated bitset field
    // indicating which columns are unset and which are tombstones this is only used for CDC
    UPDATED_FIELDS_INDICATOR
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.BinaryType;
            }

            @Override
            public RowBuilder decorate(RowBuilder builder)
            {
                return new UpdatedFieldsIndicatorDecorator(builder);
            }
        },

    // Special column that passes over boolean field
    // marking if mutation was an UPDATE or an INSERT this is only used for CDC
    UPDATE_FLAG
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.BooleanType;
            }

            @Override
            public RowBuilder decorate(RowBuilder builder)
            {
                return new UpdateFlagDecorator(builder);
            }
        },

    // Feature column that contains the column_name
    // to a list of keys of the tombstoned values in a complex data type this is only used for CDC
    CELL_DELETION_IN_COMPLEX
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.BinaryType));
            }

            @Override
            public RowBuilder decorate(RowBuilder builder)
            {
                return new CellTombstonesInComplexDecorator(builder);
            }
        },

    RANGE_DELETION
        {
            private transient DataType dataType;

            @Override
            public void generateDataType(CqlTable table, StructType sparkSchema)
            {
                // When there is no clustering keys, range deletion won't happen;
                // such deletion applies only when there are clustering key(s)
                if (table.numClusteringKeys() == 0)
                {
                    // Assign a dummy data type, it won't be reach with such CQL schema
                    dataType = DataTypes.BooleanType;
                    return;
                }

                List<StructField> clusteringKeyFields = table.clusteringKeys().stream()
                                                             .map(cqlField -> sparkSchema.apply(cqlField.name()))
                                                             .collect(Collectors.toList());
                StructType clusteringKeys = DataTypes.createStructType(clusteringKeyFields);
                StructField[] rangeTombstone = new StructField[RangeTombstoneMarker.TOTAL_FIELDS];
                // The array of binaries follows the same seq of the clustering key definition, e.g. for primary key
                // (pk, ck1, ck2), the array value could be [ck1] or [ck1, ck2], but never (ck2) without ck1
                rangeTombstone[RangeTombstoneMarker.START_FIELD_POSITION] =
                        DataTypes.createStructField("Start", clusteringKeys, true);
                // Default to be inclusive if null
                rangeTombstone[RangeTombstoneMarker.START_INCLUSIVE_FIELD_POSITION] =
                        DataTypes.createStructField("StartInclusive", DataTypes.BooleanType, true);
                rangeTombstone[RangeTombstoneMarker.END_FIELD_POSITION] =
                        DataTypes.createStructField("End", clusteringKeys, true);
                // Default to be inclusive if null
                rangeTombstone[RangeTombstoneMarker.END_INCLUSIVE_FIELD_POSITION] =
                        DataTypes.createStructField("EndInclusive", DataTypes.BooleanType, true);
                dataType = DataTypes.createArrayType(DataTypes.createStructType(rangeTombstone));
            }

            @Override
            public DataType fieldDataType()
            {
                Preconditions.checkNotNull(dataType, "The dynamic data type is not initialized");
                return dataType;
            }

            @Override
            public RowBuilder decorate(RowBuilder builder)
            {
                return new RangeTombstoneDecorator(builder);
            }
        };

    public static final List<SchemaFeature> ALL_CDC_FEATURES = ImmutableList.of(UPDATED_FIELDS_INDICATOR,
                                                                                UPDATE_FLAG,
                                                                                CELL_DELETION_IN_COMPLEX,
                                                                                RANGE_DELETION);

    /**
     * Initialize the requested features from the input options
     *
     * @param options
     * @return the requested features list. If none is requested, an empty list is returned
     */
    public static List<SchemaFeature> initializeFromOptions(Map<String, String> options)
    {
        // TODO: Some features are only valid for the CDC scenario; probably reject early
        return Arrays.stream(values())
                     .filter(feature -> Boolean.parseBoolean(options.getOrDefault(feature.optionName(), "false").toLowerCase()))
                     .collect(Collectors.toList());
    }
}
