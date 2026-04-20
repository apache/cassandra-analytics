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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.avro.Schema;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroSchemaUtilsTest
{
    /**
     * Verifies that the merged schema:
     * <ul>
     *   <li>contains all envelope fields except the {@code payload} placeholder</li>
     *   <li>has the supplied payload schema as the last field named {@code payload}</li>
     *   <li>produces the same schema fingerprint on repeated calls with identical inputs
     *       (deterministic, as required for consistent {@code schemaUuid} Kafka headers)</li>
     * </ul>
     */
    @Test
    public void buildMergedSchemaCorrectStructureAndDeterministicFingerprint() throws IOException
    {
        Schema envelope = new Schema.Parser().parse(
                AvroSchemaUtilsTest.class.getClassLoader().getResourceAsStream("cdc_bytes.avsc"));

        Schema payloadSchema = Schema.createRecord("Payload", null, "test.payload", false);
        payloadSchema.setFields(Collections.singletonList(
                new Schema.Field("id", Schema.create(Schema.Type.INT), null)));

        Schema merged = AvroSchemaUtils.buildMergedSchema(envelope, payloadSchema, "TestRecord", "test.ns");

        // payload field is the last field
        List<Schema.Field> fields = merged.getFields();
        assertThat(fields.get(fields.size() - 1).name()).isEqualTo(AvroConstants.PAYLOAD_KEY);
        assertThat(merged.getField(AvroConstants.PAYLOAD_KEY).schema()).isEqualTo(payloadSchema);

        // All envelope fields (except the replaced payload placeholder) are present
        Set<String> mergedFieldNames = fields.stream().map(Schema.Field::name).collect(Collectors.toSet());
        for (Schema.Field f : envelope.getFields())
        {
            if (!f.name().equals(AvroConstants.PAYLOAD_KEY))
            {
                assertThat(mergedFieldNames).as("Envelope field '%s' must be in merged schema", f.name())
                                            .contains(f.name());
            }
        }

        // Schema fingerprint is deterministic: same inputs produce the same UUID
        Schema merged2 = AvroSchemaUtils.buildMergedSchema(envelope, payloadSchema, "TestRecord", "test.ns");
        String uuid1 = UUID.nameUUIDFromBytes(merged.toString().getBytes(StandardCharsets.UTF_8)).toString();
        String uuid2 = UUID.nameUUIDFromBytes(merged2.toString().getBytes(StandardCharsets.UTF_8)).toString();
        assertThat(uuid1).isEqualTo(uuid2);
    }
}
