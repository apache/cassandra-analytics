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
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;

/**
 * Sorts the elements of the sets and the entries of the maps in serialized values, at any depth, as Cassandra
 * sorts those of a CQL literal: by the comparator of the element or key type, keeping the first of equal elements
 * and the last value of equal keys.
 * <p>
 * Values are in the format of the native protocol from version 3 on, in which Cassandra also stores frozen
 * collections, tuples and user-defined types. This class only uses the parts of the Cassandra API that are the
 * same from Cassandra 4.0 on, so that the bridges of all versions share it. A subclass sorts the values of the
 * types that a later version adds, by overriding {@link #sortComponents}.
 * <p>
 * Thread-safe.
 */
class CollectionElementSorter
{
    static final CollectionElementSorter INSTANCE = new CollectionElementSorter();

    /**
     * @param type type of a value
     * @return whether a value of {@code type} holds a set or a map, at any depth below the value itself
     */
    boolean nestsSetOrMap(AbstractType<?> type)
    {
        for (AbstractType<?> subType : unwrap(type).subTypes())
        {
            if (holdsSetOrMap(subType))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * @param type  type of the value
     * @param value serialized value; it is not modified
     * @return {@code value}, with the elements of its sets and the entries of its maps sorted at any depth
     */
    ByteBuffer sort(AbstractType<?> type, ByteBuffer value)
    {
        AbstractType<?> valueType = unwrap(type);
        if (value == null || !value.hasRemaining() || !holdsSetOrMap(valueType))
        {
            return value;
        }
        if (valueType instanceof SetType)
        {
            return sortSet((SetType<?>) valueType, value);
        }
        if (valueType instanceof MapType)
        {
            return sortMap((MapType<?, ?>) valueType, value);
        }
        if (valueType instanceof ListType)
        {
            return sortList((ListType<?>) valueType, value);
        }
        if (valueType instanceof TupleType)
        {
            // A user-defined type is a tuple type
            return sortTuple((TupleType) valueType, value);
        }
        return sortComponents(valueType, value);
    }

    /**
     * Sorts a value of a type that holds a set or a map, and that is not a collection, tuple or user-defined type
     *
     * @param type  type of the value
     * @param value serialized value, not empty; it is not modified
     * @return {@code value}, with the elements of its sets and the entries of its maps sorted at any depth
     */
    protected ByteBuffer sortComponents(AbstractType<?> type, ByteBuffer value)
    {
        throw new IllegalArgumentException("Unable to sort the sets and maps in a value of type " + type.asCQL3Type());
    }

    private boolean holdsSetOrMap(AbstractType<?> type)
    {
        AbstractType<?> valueType = unwrap(type);
        return valueType instanceof SetType || valueType instanceof MapType || nestsSetOrMap(valueType);
    }

    private ByteBuffer sortSet(SetType<?> type, ByteBuffer value)
    {
        AbstractType<?> elementType = type.getElementsType();
        // TreeSet#add keeps the first of equal elements, as Cassandra does when it binds a set
        TreeSet<ByteBuffer> elements = new TreeSet<>(elementType);
        for (ByteBuffer element : readCollection(value, 1))
        {
            elements.add(sort(elementType, element));
        }
        return writeCollection(elements.size(), new ArrayList<>(elements));
    }

    private ByteBuffer sortMap(MapType<?, ?> type, ByteBuffer value)
    {
        AbstractType<?> keyType = type.getKeysType();
        AbstractType<?> valueType = type.getValuesType();
        // TreeMap#put keeps the last value of equal keys, as Cassandra does when it binds a map
        TreeMap<ByteBuffer, ByteBuffer> entries = new TreeMap<>(keyType);
        List<ByteBuffer> keysAndValues = readCollection(value, 2);
        for (int index = 0; index < keysAndValues.size(); index += 2)
        {
            entries.put(sort(keyType, keysAndValues.get(index)), sort(valueType, keysAndValues.get(index + 1)));
        }
        List<ByteBuffer> sorted = new ArrayList<>(entries.size() * 2);
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet())
        {
            sorted.add(entry.getKey());
            sorted.add(entry.getValue());
        }
        return writeCollection(entries.size(), sorted);
    }

    private ByteBuffer sortList(ListType<?> type, ByteBuffer value)
    {
        AbstractType<?> elementType = type.getElementsType();
        List<ByteBuffer> elements = readCollection(value, 1);
        elements.replaceAll(element -> sort(elementType, element));
        return writeCollection(elements.size(), elements);
    }

    private ByteBuffer sortTuple(TupleType type, ByteBuffer value)
    {
        // A tuple value is its components, each preceded by its length, or by -1 for a null component.
        // A value may have fewer components than its type, but not more.
        ByteBuffer input = value.duplicate();
        List<ByteBuffer> components = new ArrayList<>(type.size());
        while (input.hasRemaining())
        {
            if (components.size() == type.size())
            {
                throw new IllegalArgumentException("Invalid value of type " + type.asCQL3Type() + ": too many components");
            }
            components.add(sort(type.type(components.size()), readValue(input)));
        }
        return writeValues(0, components, false);
    }

    /**
     * @param value             serialized collection: its number of elements, then each element preceded by its length
     * @param valuesPerElement  number of values that make up each element: 2 for the key and value of a map entry
     * @return the values of the collection, in their serialized order
     */
    private static List<ByteBuffer> readCollection(ByteBuffer value, int valuesPerElement)
    {
        ByteBuffer input = value.duplicate();
        int elements = readInt(input);
        // Each value takes at least the bytes of its length
        if (elements < 0 || elements > input.remaining() / (Integer.BYTES * valuesPerElement))
        {
            throw new IllegalArgumentException("Invalid collection value: " + elements + " elements in "
                                               + input.remaining() + " bytes");
        }
        int size = elements * valuesPerElement;
        List<ByteBuffer> values = new ArrayList<>(size);
        for (int index = 0; index < size; index++)
        {
            ByteBuffer element = readValue(input);
            if (element == null)
            {
                throw new IllegalArgumentException("Invalid collection value: null element");
            }
            values.add(element);
        }
        if (input.hasRemaining())
        {
            throw new IllegalArgumentException("Invalid collection value: " + input.remaining() + " bytes after its last element");
        }
        return values;
    }

    private static ByteBuffer readValue(ByteBuffer input)
    {
        int length = readInt(input);
        if (length < 0)
        {
            return null;
        }
        if (length > input.remaining())
        {
            throw new IllegalArgumentException("Invalid value: length of " + length + " with " + input.remaining() + " bytes left");
        }
        ByteBuffer value = input.slice();
        value.limit(length);
        input.position(input.position() + length);
        return value;
    }

    private static int readInt(ByteBuffer input)
    {
        if (input.remaining() < Integer.BYTES)
        {
            throw new IllegalArgumentException("Invalid value: " + input.remaining() + " bytes left for a length");
        }
        return input.getInt();
    }

    private static ByteBuffer writeCollection(int size, List<ByteBuffer> values)
    {
        return writeValues(size, values, true);
    }

    private static ByteBuffer writeValues(int size, List<ByteBuffer> values, boolean withSize)
    {
        int length = withSize ? Integer.BYTES : 0;
        for (ByteBuffer value : values)
        {
            length += Integer.BYTES + (value == null ? 0 : value.remaining());
        }
        ByteBuffer output = ByteBuffer.allocate(length);
        if (withSize)
        {
            output.putInt(size);
        }
        for (ByteBuffer value : values)
        {
            if (value == null)
            {
                output.putInt(-1);
            }
            else
            {
                output.putInt(value.remaining());
                output.put(value.duplicate());
            }
        }
        output.flip();
        return output;
    }

    private static AbstractType<?> unwrap(AbstractType<?> type)
    {
        return type instanceof ReversedType ? ((ReversedType<?>) type).baseType : type;
    }
}
