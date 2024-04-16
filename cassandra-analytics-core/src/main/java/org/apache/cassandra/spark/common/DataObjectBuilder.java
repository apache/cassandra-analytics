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

package org.apache.cassandra.spark.common;

import java.util.function.Consumer;

/**
 * Interface to build data objects
 * @param <T> type of builder
 * @param <R> type of result from build
 */
public interface DataObjectBuilder<T extends DataObjectBuilder<?, ?>, R>
{
    /**
     * Build into data object of type R
     * @return data object type
     */
    R build();

    /**
     * Self typing
     * @return type of implementor class
     */
    T self();

    /**
     * Update fields in builder
     * @param updater function to update fields
     * @return builder itself for chained invocation
     */
    default T with(Consumer<? super T> updater)
    {
        updater.accept(self());
        return self();
    }
}
