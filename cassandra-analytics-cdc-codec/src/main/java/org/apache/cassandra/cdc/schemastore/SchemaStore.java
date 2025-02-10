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

package org.apache.cassandra.cdc.schemastore;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;

/**
 * Interface representing a CDC schema store.
 */
public interface SchemaStore
{
    /**
     * Get the avro corresponding to the namespace and the name of the schema
     * @param namespace
     * @param name
     * @return avro schema, or return null if nothing can be found
     */
    Schema getSchema(String namespace, String name);

    /**
     * Get the schema version corresponding to the namespace and the name of the schema
     * @param namespace
     * @param name
     * @return a type 3 (name based) UUID generated based on the MD5 of the CQL schema,
     *      or return null if nothing can be found
     */
    @Nullable
    default String getVersion(String namespace, String name)
    {
        Schema schema = getSchema(namespace, name);
        if (schema == null)
        {
            return null;
        }
        return UUID.nameUUIDFromBytes(schema.toString().getBytes(StandardCharsets.UTF_8)).toString();
    }

    /**
     * Get the datum writer
     * @param namespace
     * @param name
     * @return datum writer or null if schema is not found
     */
    GenericDatumWriter<GenericRecord> getWriter(String namespace, String name);

    /**
     * Get the datum reader
     * @param namespace
     * @param name
     * @return datum reader or null if schema is not found
     */
    GenericDatumReader<GenericRecord> getReader(String namespace, String name);
}
