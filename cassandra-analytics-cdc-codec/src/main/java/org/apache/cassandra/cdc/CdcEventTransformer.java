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

package org.apache.cassandra.cdc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.cassandra.cdc.api.KeyspaceTypeKey;
import org.apache.cassandra.cdc.msg.CdcEvent;
import org.apache.cassandra.cdc.schemastore.SchemaStore;
import org.apache.cassandra.spark.data.CqlField;

/**
 * Base abstraction to convert CdcEvent objects into another data format, e.g. Avro, Json etc
 *
 * @param <T> the type resulted as a result of the transformation.
 */
public abstract class CdcEventTransformer<T>
{
    public final Schema cdcSchema;
    public final Schema ttlSchema;
    public final Schema rangeSchema;

    protected final BinaryEncoder encoder;
    protected final Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup;
    protected final SchemaStore store;

    public CdcEventTransformer(SchemaStore store,
                               Function<KeyspaceTypeKey, CqlField.CqlType> typeLookup,
                               String templatePath)
    {
        this.cdcSchema = readSchema(templatePath);
        this.ttlSchema = extractTtlSchema(cdcSchema);
        this.rangeSchema = extractRangeSchema(cdcSchema);
        this.encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(0), null);
        this.typeLookup = typeLookup;
        this.store = store;
    }

    public abstract T transform(CdcEvent event);

    private static Schema readSchema(String filename)
    {
        ClassLoader classLoader = CdcEventTransformer.class.getClassLoader();
        final InputStream is = classLoader.getResourceAsStream(filename);
        try
        {
            return new Schema.Parser().parse(is);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Schema extractTtlSchema(Schema cdcSchema)
    {
        List<Schema> nullableTtlUnion = cdcSchema.getField("ttl").schema().getTypes();
        return nullableTtlUnion.stream()
                               .filter(s -> s.getType() == Schema.Type.RECORD)
                               .findFirst()
                               .get(); // the field exist. see cdc.avsc file
    }

    private static Schema extractRangeSchema(Schema cdcSchema)
    {
        List<Schema> nullableRangeUnion = cdcSchema.getField("range").schema().getTypes();
        return nullableRangeUnion.stream()
                                 .filter(s -> s.getType() == Schema.Type.ARRAY)
                                 .map(Schema::getElementType)
                                 .findFirst()
                                 .get(); // the field exist. see cdc.avsc file
    }

    public byte[] encode(GenericDatumWriter<GenericRecord> writer, GenericData.Record update)
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, encoder);
        try
        {
            writer.write(update, binaryEncoder);
            binaryEncoder.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }
}
