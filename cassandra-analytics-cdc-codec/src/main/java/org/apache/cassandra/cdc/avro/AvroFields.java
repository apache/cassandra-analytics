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

package org.apache.cassandra.cdc.avro;

public final class AvroFields
{
    public static final String PAYLOAD_KEY = "payload";
    public static final String TIMESTAMP_KEY = "timestampMicros";
    public static final String VERSION_KEY = "version";
    public static final String IS_PARTIAL_KEY = "isPartial";
    public static final String SOURCE_TABLE_KEY = "sourceTable";
    public static final String SOURCE_KEYSPACE_KEY = "sourceKeyspace";
    public static final String OPERATION_TYPE_KEY = "operationType";
    public static final String UPDATE_FIELDS_KEY = "updateFields";
    public static final String RANGE_KEY = "range";
    public static final String TTL_KEY = "ttl";

    public static final String CURRENT_VERSION = "2";

    private AvroFields()
    {

    }
}
