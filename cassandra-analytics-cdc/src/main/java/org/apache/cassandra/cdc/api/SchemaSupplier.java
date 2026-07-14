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

package org.apache.cassandra.cdc.api;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.spark.data.CqlTable;

/**
 * Supplies schema for all tables relevant to CDC processing.
 * Returns ALL tables (CDC-enabled and CDC-disabled) so that the bridge's Schema.instance
 * is complete enough to deserialize any commit log mutation without UnknownTableException.
 * Callers use {@link org.apache.cassandra.spark.data.CqlTable#cdc()} to filter for publishing.
 */
public interface SchemaSupplier
{
    CompletableFuture<Set<CqlTable>> getTables();
}
