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

package org.apache.cassandra.spark.data.partitioner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.SparkSSTableReader;

/**
 * This class is SSTable supplier replicas in multiple data centers.
 * This type of supplier is useful for EACH_QUORUM consistency level.
 */
public class MultiDCReplicas extends SSTablesSupplier
{
    private final Map<String, SSTablesSupplier> replicasPerDC;

    public MultiDCReplicas(Map<String, SSTablesSupplier> replicasPerDC)
    {
        if (replicasPerDC == null || replicasPerDC.isEmpty())
        {
            throw new IllegalArgumentException("replicasPerDC cannot be null or empty");
        }
        this.replicasPerDC = replicasPerDC;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends SparkSSTableReader> Set<T> openAll(ReaderOpener<T> readerOpener)
    {
        Set<T> combinedReaders = new HashSet<>();
        replicasPerDC.values()
                     .forEach(supplier -> combinedReaders.addAll(supplier.openAll(readerOpener)));
        return combinedReaders;
    }
}
