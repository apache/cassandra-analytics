/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Utilities for data generation used for tests
 */
public class DataGenerationUtils
{

    /**
     * Generates course data with schema
     *
     * <pre>
     *     id integer,
     *     course string,
     *     marks integer
     * </pre>
     *
     * @param spark    the spark session to use
     * @param rowCount the number of records to generate
     * @return a {@link Dataset} with generated data
     */
    public static Dataset<Row> generateCourseData(SparkSession spark, int rowCount)
    {
        SQLContext sql = spark.sqlContext();
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("course", StringType, false)
                            .add("marks", IntegerType, false);

        List<Row> rows = IntStream.range(0, rowCount)
                                  .mapToObj(recordNum -> {
                                      String course = "course" + recordNum;
                                      Object[] values = { recordNum, course, recordNum };
                                      return RowFactory.create(values);
                                  }).collect(Collectors.toList());
        return sql.createDataFrame(rows, schema);
    }

    public static Dataset<Row> generateCourseData(SparkSession spark, Integer ttl, Long timestamp, int rowCount)
    {
        SQLContext sql = spark.sqlContext();
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("course", StringType, false)
                            .add("marks", IntegerType, false);

        if (ttl != null)
        {
            schema = schema.add("ttl", IntegerType, false);
        }
        if (timestamp != null)
        {
            schema = schema.add("timestamp", LongType, false);
        }

        List<Row> rows = IntStream.range(0, rowCount)
                                  .mapToObj(recordNum -> {
                                      String courseNameString = "course" + recordNum;
                                      int courseNameStringLen = courseNameString.length();
                                      int courseNameMultiplier = 1000 / courseNameStringLen;
                                      String courseName = dupString(courseNameString, courseNameMultiplier);
                                      List<Object> baseValues = Arrays.asList(recordNum, courseName, recordNum);
                                      List<Object> values = new ArrayList<>(baseValues);
                                      if (ttl != null)
                                      {
                                          values.add(ttl);
                                      }
                                      if (timestamp != null)
                                      {
                                          values.add(timestamp);
                                      }
                                      return RowFactory.create(values.toArray());
                                  }).collect(Collectors.toList());
        return sql.createDataFrame(rows, schema);
    }

    private static String dupString(String string, Integer times)
    {
        byte[] stringBytes = string.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(stringBytes.length * times);
        for (int time = 0; time < times; time++)
        {
            buffer.put(stringBytes);
        }
        return new String(buffer.array(), StandardCharsets.UTF_8);
    }
}
