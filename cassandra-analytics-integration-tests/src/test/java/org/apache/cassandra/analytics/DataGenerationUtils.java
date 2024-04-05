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
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructType;

/**
 * Utilities for data generation used for tests
 */
public final class DataGenerationUtils
{
    private DataGenerationUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Generates course data with schema
     * Does not generate a User Defined Field
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
        return generateCourseData(spark, rowCount, false, null, null);
    }

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
     * @param udfData  if a field representing a User Defined Type should be added
     * @return a {@link Dataset} with generated data
     */
    public static Dataset<Row> generateCourseData(SparkSession spark, int rowCount, boolean udfData)
    {
        return generateCourseData(spark, rowCount, udfData, null, null);
    }

    /**
     * Generates course data with schema
     *
     * <pre>
     *     id integer,
     *     course string,
     *     marks integer
     * </pre>
     *
     * @param spark     the spark session to use
     * @param rowCount  the number of records to generate
     * @param udfData   if a field representing a User Defined Type should be added
     * @param ttl       (optional) a TTL value for the data frame
     * @param timestamp (optional) a timestamp value for the data frame
     * @return a {@link Dataset} with generated data
     */
    public static Dataset<Row> generateCourseData(SparkSession spark, int rowCount, boolean udfData,
                                                  Integer ttl, Long timestamp)
    {
        SQLContext sql = spark.sqlContext();
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("course", StringType, false)
                            .add("marks", IntegerType, false);
        if (udfData)
        {
            StructType udfType = new StructType()
                                 .add("TimE", IntegerType, false)
                                 .add("limit", IntegerType, false);
            schema = schema.add("User_Defined_Type", udfType);
        }

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
                                      String course = "course" + recordNum;
                                      List<Object> values = new ArrayList<>(Arrays.asList(recordNum, course, recordNum));
                                      if (udfData)
                                      {
                                          values.add(RowFactory.create(recordNum, recordNum));
                                      }
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

    public static Dataset<Row> generateUdtData(SparkSession spark, int rowCount)
    {
        SQLContext sql = spark.sqlContext();
        StructType udtType = createStructType(new StructField[]{new StructField("f1", StringType, false, Metadata.empty()),
                                                                new StructField("f2", IntegerType, false, Metadata.empty())});
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("udtfield", udtType, false);

        List<Row> rows = IntStream.range(0, rowCount)
                                  .mapToObj(id -> {
                                      String course = "course" + id;
                                      Object[] values = {id, RowFactory.create(course, id)};
                                      return RowFactory.create(values);
                                  }).collect(Collectors.toList());
        return sql.createDataFrame(rows, schema);
    }

    public static Dataset<Row> generateNestedUdtData(SparkSession spark, int rowCount)
    {
        SQLContext sql = spark.sqlContext();
        StructType udtType = createStructType(new StructField[]{new StructField("f1", StringType, false, Metadata.empty()),
                                                                new StructField("f2", IntegerType, false, Metadata.empty())});
        StructType nestedType = createStructType(new StructField[] {new StructField("n1", IntegerType, false, Metadata.empty()),
                                                                    new StructField("n2", udtType, false, Metadata.empty())});
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("nested", nestedType, false);

        List<Row> rows = IntStream.range(0, rowCount)
                                  .mapToObj(id -> {
                                      String course = "course" + id;
                                      Row innerUdt = RowFactory.create(id, RowFactory.create(course, id));
                                      Object[] values = {id, innerUdt};
                                      return RowFactory.create(values);
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
