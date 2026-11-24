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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CollectionElementSorterTest
{
    private static final CollectionElementSorter SORTER = CollectionElementSorter.INSTANCE;
    private static final SetType<String> TEXT_SET = SetType.getInstance(UTF8Type.instance, false);
    private static final MapType<String, Integer> TEXT_INT_MAP = MapType.getInstance(UTF8Type.instance, Int32Type.instance, false);
    private static final TupleType INT_TEXT_SET_TEXT = new TupleType(Arrays.asList(Int32Type.instance, TEXT_SET, UTF8Type.instance));

    @Test
    void nestsSetOrMap()
    {
        assertThat(SORTER.nestsSetOrMap(INT_TEXT_SET_TEXT)).isTrue();
        assertThat(SORTER.nestsSetOrMap(ListType.getInstance(TEXT_SET, false))).isTrue();
        assertThat(SORTER.nestsSetOrMap(SetType.getInstance(TEXT_SET, false))).isTrue();
        assertThat(SORTER.nestsSetOrMap(MapType.getInstance(Int32Type.instance, TEXT_INT_MAP, false))).isTrue();
        assertThat(SORTER.nestsSetOrMap(ReversedType.getInstance(INT_TEXT_SET_TEXT))).isTrue();

        // Cassandra sorts a set or a map that is the whole value
        assertThat(SORTER.nestsSetOrMap(TEXT_SET)).isFalse();
        assertThat(SORTER.nestsSetOrMap(TEXT_INT_MAP)).isFalse();
        assertThat(SORTER.nestsSetOrMap(ReversedType.getInstance(TEXT_SET))).isFalse();
        assertThat(SORTER.nestsSetOrMap(ListType.getInstance(Int32Type.instance, false))).isFalse();
        assertThat(SORTER.nestsSetOrMap(new TupleType(Arrays.asList(Int32Type.instance, UTF8Type.instance)))).isFalse();
        assertThat(SORTER.nestsSetOrMap(Int32Type.instance)).isFalse();
    }

    @Test
    void sortsSetsAndMapsAtAnyDepth()
    {
        TupleType type = new TupleType(Arrays.asList(ListType.getInstance(TEXT_SET, false), TEXT_INT_MAP));
        ByteBuffer value = tuple(collection(collection(text("p"), text("a")), collection(text("z"), text("b"))),
                                 map(text("p"), int32(2), text("a"), int32(1)));

        assertThat(SORTER.sort(type, value))
        .isEqualTo(tuple(collection(collection(text("a"), text("p")), collection(text("b"), text("z"))),
                         map(text("a"), int32(1), text("p"), int32(2))));
    }

    @Test
    void sortsByTheComparatorOfTheElementType()
    {
        // Int32Type compares signed values, while their bytes compare unsigned
        SetType<Integer> intSet = SetType.getInstance(Int32Type.instance, false);
        TupleType type = new TupleType(Collections.singletonList(intSet));

        assertThat(SORTER.sort(type, tuple(collection(int32(1), int32(-1)))))
        .isEqualTo(tuple(collection(int32(-1), int32(1))));
    }

    @Test
    void keepsTheFirstOfEqualElements()
    {
        // Cassandra keeps the first of the set elements that compare equal, such as 1.0 and 1.00
        SetType<BigDecimal> decimalSet = SetType.getInstance(DecimalType.instance, false);
        TupleType type = new TupleType(Collections.singletonList(decimalSet));
        ByteBuffer value = tuple(collection(decimal("2"), decimal("1.00"), decimal("1.0")));

        assertThat(SORTER.sort(type, value)).isEqualTo(tuple(collection(decimal("1.00"), decimal("2"))));
    }

    @Test
    void collapsesElementsThatAreEqualOnceSorted()
    {
        SetType<?> type = SetType.getInstance(SetType.getInstance(UTF8Type.instance, false), false);
        ByteBuffer value = collection(collection(text("p"), text("a")), collection(text("a"), text("p")));

        assertThat(SORTER.sort(type, value)).isEqualTo(collection(collection(text("a"), text("p"))));
    }

    @Test
    void keepsTheLastValueOfKeysThatAreEqualOnceSorted()
    {
        MapType<?, ?> type = MapType.getInstance(TEXT_SET, Int32Type.instance, false);
        ByteBuffer value = map(collection(text("p"), text("a")), int32(1), collection(text("a"), text("p")), int32(2));

        assertThat(SORTER.sort(type, value)).isEqualTo(map(collection(text("a"), text("p")), int32(2)));
    }

    @Test
    void keepsNullAndMissingTupleComponents()
    {
        ByteBuffer value = tuple(null, collection(text("p"), text("a")));

        assertThat(SORTER.sort(INT_TEXT_SET_TEXT, value)).isEqualTo(tuple(null, collection(text("a"), text("p"))));
    }

    @Test
    void sortsAValueOfADescendingClusteringColumn()
    {
        ByteBuffer value = tuple(int32(1), collection(text("p"), text("a")), text("x"));

        assertThat(SORTER.sort(ReversedType.getInstance(INT_TEXT_SET_TEXT), value))
        .isEqualTo(tuple(int32(1), collection(text("a"), text("p")), text("x")));
    }

    @Test
    void returnsAValueWithNothingToSortAsItIs()
    {
        ByteBuffer value = tuple(int32(1), text("x"));

        assertThat(SORTER.sort(new TupleType(Arrays.asList(Int32Type.instance, UTF8Type.instance)), value)).isSameAs(value);
        assertThat(SORTER.sort(INT_TEXT_SET_TEXT, null)).isNull();
    }

    @Test
    void doesNotModifyTheValue()
    {
        ByteBuffer value = tuple(int32(1), collection(text("p"), text("a")), text("x"));
        ByteBuffer copy = ByteBuffer.allocate(value.remaining()).put(value.duplicate());
        copy.flip();

        SORTER.sort(INT_TEXT_SET_TEXT, value);

        assertThat(value).isEqualTo(copy);
        assertThat(value.position()).isZero();
    }

    @Test
    void rejectsInvalidValues()
    {
        ByteBuffer truncated = collection(text("p"), text("a"));
        truncated.limit(truncated.limit() - 1);
        assertThatThrownBy(() -> SORTER.sort(SetType.getInstance(TEXT_SET, false), collection(truncated)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid value");

        ByteBuffer hugeCount = ByteBuffer.allocate(Integer.BYTES).putInt(Integer.MAX_VALUE);
        hugeCount.flip();
        assertThatThrownBy(() -> SORTER.sort(SetType.getInstance(TEXT_SET, false), hugeCount))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid collection value: 2147483647 elements in 0 bytes");

        TupleType single = new TupleType(Collections.singletonList(TEXT_SET));
        assertThatThrownBy(() -> SORTER.sort(single, tuple(collection(text("a")), text("extra"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("too many components");
    }

    @Test
    void canonicalValueConverterConvertsOnlyTheColumnsThatNeedIt()
    {
        Map<String, AbstractType<?>> columnTypes = new HashMap<>();
        columnTypes.put("plain", TEXT_SET);
        columnTypes.put("nested", ListType.getInstance(TEXT_SET, false));
        CanonicalValueConverter converter = new CanonicalValueConverterImplementation(columnTypes,
                                                                                      type -> UDHelper.codecFor(UDHelper.driverType(type)),
                                                                                      SORTER);
        LinkedHashSet<String> unsorted = new LinkedHashSet<>(Arrays.asList("p", "a"));
        List<LinkedHashSet<String>> nested = Collections.singletonList(unsorted);

        assertThat(converter.convert("plain", unsorted)).isSameAs(unsorted);
        assertThat(converter.convert("unknown", unsorted)).isSameAs(unsorted);
        assertThat(converter.convert("nested", null)).isNull();
        ByteBuffer unset = ByteBuffer.allocate(0);
        assertThat(converter.convert("nested", unset)).isSameAs(unset);
        Object converted = converter.convert("nested", nested);
        assertThat(converted).isEqualTo(nested);
        List<Object> elements = new ArrayList<>((Collection<?>) ((List<?>) converted).get(0));
        assertThat(elements).containsExactly("a", "p");
        assertThat(unsorted).containsExactly("p", "a");
        assertThatThrownBy(() -> converter.convert("nested", 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to convert a value of java.lang.Integer for column nested")
        .satisfies(exception -> assertThat(exception.getCause().getSuppressed()).hasSize(1));
    }

    private static ByteBuffer text(String value)
    {
        return UTF8Type.instance.decompose(value);
    }

    private static ByteBuffer int32(int value)
    {
        return Int32Type.instance.decompose(value);
    }

    private static ByteBuffer decimal(String value)
    {
        return DecimalType.instance.decompose(new BigDecimal(value));
    }

    private static ByteBuffer collection(ByteBuffer... elements)
    {
        return values(elements.length, elements);
    }

    private static ByteBuffer map(ByteBuffer... keysAndValues)
    {
        return values(keysAndValues.length / 2, keysAndValues);
    }

    private static ByteBuffer tuple(ByteBuffer... components)
    {
        return values(-1, components);
    }

    /**
     * @return the values, each preceded by its length or by -1 for null, preceded by size unless it is negative
     */
    private static ByteBuffer values(int size, ByteBuffer... values)
    {
        int length = size < 0 ? 0 : Integer.BYTES;
        for (ByteBuffer value : values)
        {
            length += Integer.BYTES + (value == null ? 0 : value.remaining());
        }
        ByteBuffer output = ByteBuffer.allocate(length);
        if (size >= 0)
        {
            output.putInt(size);
        }
        for (ByteBuffer value : values)
        {
            output.putInt(value == null ? -1 : value.remaining());
            if (value != null)
            {
                output.put(value.duplicate());
            }
        }
        output.flip();
        return output;
    }
}
