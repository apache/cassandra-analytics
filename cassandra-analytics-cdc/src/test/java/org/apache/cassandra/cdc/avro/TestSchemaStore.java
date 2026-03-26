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

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.cdc.schemastore.SchemaStore;

/**
 * In-memory {@link SchemaStore} implementation for tests.
 * Schemas are registered manually via {@link #registerSchema(String, Schema)}.
 */
public class TestSchemaStore implements SchemaStore
{
    private final Map<String, Schema> schemas = new HashMap<>();
    private final Map<String, GenericDatumWriter<GenericRecord>> writers = new HashMap<>();
    private final Map<String, GenericDatumReader<GenericRecord>> readers = new HashMap<>();

    public void registerSchema(String namespace, Schema schema)
    {
        schemas.put(namespace, schema);
        writers.put(namespace, new GenericDatumWriter<>(schema));
        readers.put(namespace, new GenericDatumReader<>(schema));
    }

    @Override
    public Schema getSchema(String namespace, String name)
    {
        return schemas.get(namespace);
    }

    @Override
    public GenericDatumWriter<GenericRecord> getWriter(String namespace, String name)
    {
        return writers.get(namespace);
    }

    @Override
    public GenericDatumReader<GenericRecord> getReader(String namespace, String name)
    {
        return readers.get(namespace);
    }
}
