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

package org.apache.cassandra.spark.data.backup;

import java.io.Serializable;

/**
 * {@link Serializable} factory used to construct {@link BackupReader} instances. Extending
 * {@code Serializable} is load-bearing: the prebuild path captures a factory into a Spark
 * closure and Spark's closure serializer (Java-only) ships it to executors, where it runs to
 * produce a freshly-constructed reader. Implementations must not capture non-serializable state.
 */
@FunctionalInterface
public interface BackupReaderFactory extends Serializable
{
    /** Creates a {@link BackupReader} from the given configuration. */
    BackupReader create(BackupReaderConfig config);
}
