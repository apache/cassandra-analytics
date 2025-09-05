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

package org.apache.cassandra.spark.reader;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.serializers.CollectionSerializer;

/**
 * ComplexTypeBuffer is a util class for reconstructing multi-cell data into complex types such as unfrozen lists, maps, sets, or UDTs.
 * ComplexTypeBuffer buffers all the cell ByteBuffers then reconstructs as a single ByteBuffer.
 */
public abstract class ComplexTypeBuffer extends AbstractComplexTypeBuffer
{
    public ComplexTypeBuffer(int cellCount, int bufferSize)
    {
        super(cellCount, bufferSize);
    }

    public ByteBuffer pack()
    {
        return CollectionSerializer.pack(buffers, ByteBufferAccessor.instance, elements());
    }
}
