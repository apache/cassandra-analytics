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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

/**
 * Utility methods for building merged CDC Avro schemas.
 *
 * <p>The merged schema combines the fixed CDC envelope template ({@code cdc_generic_record.avsc})
 * with the dynamically generated payload schema for a specific Cassandra table.
 * This is the <em>single merge path</em> used by both the producer side
 * ({@link AvroGenericRecordTransformer}) and the sidecar schema store, ensuring
 * identical schema fingerprints across the system.
 */
public final class AvroSchemaUtils
{
    private AvroSchemaUtils()
    {
    }

    /**
     * Build a merged CDC Avro schema by injecting {@code payloadSchema} into the CDC envelope template.
     *
     * <p>All fields from {@code envelopeTemplate} (except the placeholder {@code payload},
     * {@code namespace}, and {@code name} fields) are copied verbatim, and the table-specific
     * {@code payloadSchema} is appended as the {@code payload} field.
     *
     * @param envelopeTemplate the base CDC envelope schema (e.g. parsed from {@code cdc_generic_record.avsc})
     * @param payloadSchema    the table-specific Avro schema produced by the CQL-to-Avro converter
     * @param name             the record name for the merged schema
     * @param namespace        the namespace for the merged schema
     * @return merged Avro schema
     */
    public static Schema buildMergedSchema(Schema envelopeTemplate,
                                           Schema payloadSchema,
                                           String name,
                                           String namespace)
    {
        Schema mergedSchema = Schema.createRecord(name, "schema", namespace, false);
        Schema.Field payloadField = new Schema.Field(AvroConstants.PAYLOAD_KEY, payloadSchema, AvroConstants.PAYLOAD_KEY);

        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field f : envelopeTemplate.getFields())
        {
            if (!f.name().equals(AvroConstants.PAYLOAD_KEY)
                && !f.name().equals("namespace")
                && !f.name().equals("name"))
            {
                fields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
            }
        }
        fields.add(payloadField);
        mergedSchema.setFields(fields);
        return mergedSchema;
    }
}
