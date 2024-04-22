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

package org.apache.cassandra.spark.sparksql;

import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.bulkwriter.BulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.CassandraBulkSourceRelation;
import org.apache.cassandra.spark.bulkwriter.CassandraBulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.LoadNotSupportedException;
import org.apache.cassandra.spark.utils.ScalaConversionUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import scala.collection.immutable.Map;

public class CassandraDataSink implements DataSourceRegister, CreatableRelationProvider
{
    public CassandraDataSink()
    {
        CassandraBridgeFactory.validateBridges(CassandraVersion.implementedVersions());
    }

    @Override
    @NotNull
    public String shortName()
    {
        return "cassandraBulkWrite";
    }

    /**
     * @param sqlContext the SQLContext instance
     * @param saveMode   must be {@link SaveMode#Append}
     * @param parameters the writer options
     * @param data       the data to persist into the Cassandra table
     * @throws LoadNotSupportedException if the @<code>saveMode</code> is not supported: {@link SaveMode#Overwrite},
     *                                   {@link SaveMode#ErrorIfExists}, or {@link SaveMode#Ignore}
     */
    @Override
    @NotNull
    public BaseRelation createRelation(@NotNull SQLContext sqlContext,
                                       @NotNull SaveMode saveMode,
                                       @NotNull Map<String, String> parameters,
                                       @NotNull Dataset<Row> data)
    {
        switch (saveMode)
        {
            case Append:
                // Initialize the job group ID for later use if we need to cancel the job
                // TODO: Can we get a more descriptive "description" in here from the end user somehow?
                BulkWriterContext writerContext = createBulkWriterContext(
                sqlContext.sparkContext(),
                ScalaConversionUtils.<String, String>mapAsJavaMap(parameters),
                data.schema());
                try
                {
                    JobInfo jobInfo = writerContext.job();
                    String description = "Cassandra Bulk Load for table " + jobInfo.qualifiedTableName();
                    CassandraBulkSourceRelation relation = new CassandraBulkSourceRelation(writerContext, sqlContext);
                    sqlContext.sparkContext().setJobGroup(jobInfo.getId(), description, false);
                    relation.insert(data, false);
                    return relation;
                }
                catch (Exception exception)
                {
                    throw new RuntimeException(exception);
                }
                finally
                {
                    writerContext.shutdown();
                    sqlContext.sparkContext().clearJobGroup();
                }
            case Overwrite:
                throw new LoadNotSupportedException("SaveMode.Overwrite is not supported on Cassandra as it needs privileged TRUNCATE operation");
            default:
                throw new LoadNotSupportedException("SaveMode." + saveMode + " is not supported");
        }
    }

    @NotNull
    protected BulkWriterContext createBulkWriterContext(@NotNull SparkContext sparkContext,
                                                        @NotNull java.util.Map<String, String> options,
                                                        @NotNull StructType schema)
    {
        return CassandraBulkWriterContext.fromOptions(sparkContext, options, schema);
    }
}
