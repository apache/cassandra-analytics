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

package org.apache.cassandra.bridge;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;

/**
 * A {@link CollectionElementSorter} that also sorts the sets and maps in the elements of vectors.
 * Thread-safe.
 */
class VectorAwareCollectionElementSorter extends CollectionElementSorter
{
    static final VectorAwareCollectionElementSorter VECTOR_AWARE_INSTANCE = new VectorAwareCollectionElementSorter();

    @Override
    protected ByteBuffer sortComponents(AbstractType<?> type, ByteBuffer value)
    {
        if (type instanceof VectorType)
        {
            VectorType<?> vectorType = (VectorType<?>) type;
            List<ByteBuffer> elements = new ArrayList<>(vectorType.split(value));
            elements.replaceAll(element -> sort(vectorType.elementType, element));
            return vectorType.decomposeRaw(elements);
        }
        return super.sortComponents(type, value);
    }
}
