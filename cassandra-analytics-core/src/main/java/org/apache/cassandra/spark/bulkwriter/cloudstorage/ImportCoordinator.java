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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import org.apache.cassandra.spark.exception.ImportFailedException;
import org.jetbrains.annotations.Nullable;

/**
 * A coordinator conducts import.
 * It is responsible to ensure that all bulk written data is imported into Cassandra cluster in the consistency safe way.
 * {@link ImportFailedException} is thrown upon import failure, due to various reasons like timeout, consistency level not satisfied, etc.
 */
public interface ImportCoordinator
{
    /**
     * Check whether the import operation has succeeded. The method does not block.
     * @return true if import has succeeded. When import is pending or failed, it returns false
     */
    boolean succeeded();

    /**
     * Check whether the import operation has failed. The method does not block.
     * @return ImportFailedException if import has failed; null otherwise
     */
    @Nullable
    ImportFailedException failure();

    /**
     * Wait indefinitely for the import operation to complete
     * @throws ImportFailedException import failure if failed
     */
    void await() throws ImportFailedException;
}
