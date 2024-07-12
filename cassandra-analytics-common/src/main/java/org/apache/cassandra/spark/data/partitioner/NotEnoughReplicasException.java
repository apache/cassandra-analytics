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

import java.math.BigInteger;
import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Range;

import org.jetbrains.annotations.Nullable;

public class NotEnoughReplicasException extends RuntimeException
{
    public NotEnoughReplicasException(String message)
    {
        super(message);
    }

    public NotEnoughReplicasException(@NotNull ConsistencyLevel consistencyLevel,
                                      @NotNull Range<BigInteger> range,
                                      int minRequired,
                                      int numInstances,
                                      @Nullable String dataCenter)
    {
        super(String.format("Insufficient replicas found to achieve consistency level %s for token range %s - %s, "
                          + "required %d but only %d found, dataCenter=%s",
                            consistencyLevel.name(), range.lowerEndpoint(), range.upperEndpoint(),
                            minRequired, numInstances, dataCenter));
    }

    static boolean isNotEnoughReplicasException(@Nullable Throwable throwable)
    {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause())
        {
            if (cause instanceof NotEnoughReplicasException)
            {
                return true;
            }
        }
        return false;
    }
}
