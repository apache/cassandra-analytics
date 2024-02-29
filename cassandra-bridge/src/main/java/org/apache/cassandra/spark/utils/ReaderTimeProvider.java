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

package org.apache.cassandra.spark.utils;

/**
 * A {@link TimeProvider} used by reader. It decides the reference epoch on instantiation and distribute to executors
 */
public class ReaderTimeProvider implements TimeProvider
{
    private final int referenceEpochInSeconds;

    public ReaderTimeProvider()
    {
        this.referenceEpochInSeconds = nowInSeconds();
    }

    /**
     * Constructor used for deserialization
     * @param referenceEpochInSeconds reference epoch to set
     */
    public ReaderTimeProvider(int referenceEpochInSeconds)
    {
        this.referenceEpochInSeconds = referenceEpochInSeconds;
    }

    @Override
    public int referenceEpochInSeconds()
    {
        return referenceEpochInSeconds;
    }
}
