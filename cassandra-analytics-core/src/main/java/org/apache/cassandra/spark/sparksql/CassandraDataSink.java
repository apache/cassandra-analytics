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
import org.apache.cassandra.spark.bulkwriter.BulkWriterContextFactory;
import org.apache.cassandra.spark.bulkwriter.CassandraBulkSourceRelation;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.exception.UnsupportedAnalyticsOperationException;
import org.apache.cassandra.spark.utils.ScalaConversionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.jetbrains.annotations.NotNull;

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
     * @throws UnsupportedAnalyticsOperationException if the {@code saveMode} is not supported. Only {@link SaveMode#Append} is supported
     */
    @Override
    @NotNull
    public BaseRelation createRelation(@NotNull SQLContext sqlContext,
                                       @NotNull SaveMode saveMode,
                                       @NotNull scala.collection.immutable.Map<String, String> parameters,
                                       @NotNull Dataset<Row> data)
    {
        switch (saveMode)
        {
            case Append:
                BulkWriterContext writerContext = null;
                try
                {
                    writerContext = factory().createBulkWriterContext(sqlContext.sparkContext(),
                                                                      toJavaMap(parameters),
                                                                      data.schema());
                    JobInfo jobInfo = writerContext.job();
                    CassandraBulkSourceRelation relation = new CassandraBulkSourceRelation(writerContext, sqlContext);
                    // Initialize the job group ID for later use if we need to cancel the job
                    // TODO: Can we get a more descriptive "description" in here from the end user somehow?
                    String description = "Cassandra bulk write for table " + jobInfo.qualifiedTableName();
                    sqlContext.sparkContext().setJobGroup(jobInfo.getId(), description, false);
                    relation.insert(data, false);
                    return relation;
                }
                finally
                {
                    if (writerContext != null)
                    {
                        writerContext.shutdown();
                    }
                    sqlContext.sparkContext().clearJobGroup();
                }
            case Overwrite:
                throw new UnsupportedAnalyticsOperationException("SaveMode.Overwrite is not supported on Cassandra as it needs privileged TRUNCATE operation");
            default:
                throw new UnsupportedAnalyticsOperationException("SaveMode." + saveMode + " is not supported");
        }
    }

    @NotNull
    protected BulkWriterContextFactory factory()
    {
        return new BulkWriterContextFactory();
    }

    // Util method to convert from Scala map to Java map. FQCN is used for code clarity.
    private static java.util.Map<String, String> toJavaMap(scala.collection.immutable.Map<String, String> map)
    {
        // preserve the type arguments as required by jdk 1.8
        return ScalaConversionUtils.<String, String>mapAsJavaMap(map);
    }
}
