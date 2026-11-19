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

package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.bridge.TokenRange;

public final class SSTableTokenIndex implements Serializable
{
    private static final long serialVersionUID = 2026042501L;

    private final Map<SSTableIndexKey, SSTableTokenBounds> boundsBySSTable;
    private final int successCount;
    private final int missingCount;
    private final int errorCount;

    private SSTableTokenIndex(Map<SSTableIndexKey, SSTableTokenBounds> boundsBySSTable,
                              int successCount,
                              int missingCount,
                              int errorCount)
    {
        this.boundsBySSTable = Collections.unmodifiableMap(boundsBySSTable);
        this.successCount = successCount;
        this.missingCount = missingCount;
        this.errorCount = errorCount;
    }

    public static SSTableTokenIndex fromShards(List<TokenIndexShard> shards)
    {
        Map<SSTableIndexKey, SSTableTokenBounds> merged = new HashMap<>();
        int successCount = 0;
        int missingCount = 0;
        int errorCount = 0;
        for (TokenIndexShard shard : shards)
        {
            merged.putAll(shard.boundsBySSTable());
            successCount += shard.successCount();
            missingCount += shard.missingCount();
            errorCount += shard.errorCount();
        }
        return new SSTableTokenIndex(merged, successCount, missingCount, errorCount);
    }

    public boolean include(SSTableKey key, TokenRange range)
    {
        SSTableTokenBounds bounds = boundsBySSTable.get(SSTableIndexKey.from(key));
        return bounds == null || bounds.overlaps(range);
    }

    public int size()
    {
        return boundsBySSTable.size();
    }

    public int successCount()
    {
        return successCount;
    }

    public int missingCount()
    {
        return missingCount;
    }

    public int errorCount()
    {
        return errorCount;
    }

    public long estimatedSizeInBytes()
    {
        return 64L * Math.max(1, boundsBySSTable.size());
    }
}
