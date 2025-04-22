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

package org.apache.cassandra.cdc.json;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.IdentityHashMap;

import org.apache.avro.generic.GenericData;

/**
 * Mostly the same with its parent class `GenericData`, except encoding the bytes in the Base64 format
 */
public class CdcGenericData extends GenericData
{
    private static final CdcGenericData INSTANCE = new CdcGenericData();

    public static CdcGenericData getInstance()
    {
        return INSTANCE;
    }

    @Override
    protected void toString(Object datum, StringBuilder buffer, IdentityHashMap<Object, Object> seenObjects)
    {
        if (isBytes(datum))
        {
            buffer.append('"');
            ByteBuffer bytes = ((ByteBuffer) datum).duplicate();
            ByteBuffer encoded = Base64.getEncoder().encode(bytes);
            buffer.append(StandardCharsets.UTF_8.decode(encoded));
            buffer.append('"');
        }
        else
        {
            super.toString(datum, buffer, seenObjects);
        }
    }
}
