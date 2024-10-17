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

package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({ "WeakerAccess", "unused" })
public class CqlField implements Serializable, Comparable<CqlField>
{
    private static final long serialVersionUID = 42L;
    public static final int NO_TTL = 0;

    public static final Comparator<String> STRING_COMPARATOR = String::compareTo;
    public static final Comparator<Byte> BYTE_COMPARATOR = CqlField::compareBytes;
    public static final Comparator<Long> LONG_COMPARATOR = Long::compareTo;
    public static final Comparator<Integer> INTEGER_COMPARATOR = Integer::compareTo;
    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = UnsignedBytes.lexicographicalComparator();
    public static final Comparator<Boolean> BOOLEAN_COMPARATOR = Boolean::compareTo;
    public static final Comparator<Double> DOUBLE_COMPARATOR = Double::compareTo;
    public static final Comparator<Void> VOID_COMPARATOR_COMPARATOR = (first, second) -> 0;
    public static final Comparator<Float> FLOAT_COMPARATOR = Float::compareTo;
    public static final Comparator<Short> SHORT_COMPARATOR = Short::compare;
    public static final Comparator<String> UUID_COMPARATOR = Comparator.comparing(java.util.UUID::fromString);
    public static final Comparator<BigDecimal> BIGDECIMAL_COMPARATOR = Comparator.naturalOrder();

    private static int compareBytes(byte first, byte second)
    {
        return first - second;  // Safe because of the range being restricted
    }

    public interface CqlType extends Serializable
    {
        enum InternalType
        {
            NativeCql, Set, List, Map, Frozen, Udt, Tuple;

            public static InternalType fromString(String name)
            {
                switch (name.toLowerCase())
                {
                    case "set":
                        return Set;
                    case "list":
                        return List;
                    case "map":
                        return Map;
                    case "tuple":
                        return Tuple;
                    case "udt":
                        return Udt;
                    case "frozen":
                        return Frozen;
                    default:
                        return NativeCql;
                }
            }
        }

        boolean isSupported();

        /**
         * @return true if type is frozen
         */
        default boolean isFrozen()
        {
            return false;
        }

        /**
         * @return true if type is complex.
         */
        default boolean isComplex()
        {
            return false;
        }

        default Object deserializeToType(TypeConverter converter, ByteBuffer buffer)
        {
            return deserializeToType(converter, buffer, isFrozen());
        }

        default Object deserializeToType(TypeConverter converter, ByteBuffer buffer, boolean isFrozen)
        {
            Object value = deserializeToJavaType(buffer, isFrozen);
            return value != null ? converter.convert(this, value, isFrozen) : null;
        }

        default Object deserializeToJavaType(ByteBuffer buffer)
        {
            return deserializeToJavaType(buffer, isFrozen());
        }

        Object deserializeToJavaType(ByteBuffer buffer, boolean isFrozen);

        ByteBuffer serialize(Object value);

        CassandraVersion version();

        InternalType internalType();

        String name();

        String cqlName();

        void write(Output output);

        Set<CqlField.CqlUdt> udts();

        @VisibleForTesting
        int cardinality(int orElse);

        default Object randomValue()
        {
            return randomValue(RandomUtils.MIN_COLLECTION_SIZE);
        }

        @VisibleForTesting
        Object randomValue(int minCollectionSize);

        @VisibleForTesting
        Object convertForCqlWriter(Object value, CassandraVersion version);

        // Kryo Serialization

        static void write(CqlType type, Output out)
        {
            out.writeInt(type.internalType().ordinal());
        }

        static CqlType read(Input input, CassandraTypes cassandraTypes)
        {
            InternalType internalType = InternalType.values()[input.readInt()];
            return cassandraTypes.readType(internalType, input);
        }
    }

    public interface NativeType extends CqlType
    {
    }

    public interface CqlCustom extends CqlType
    {
        /**
         * @return the fully qualified name of the subtype of {@code org.apache.cassandra.db.marshal.AbstractType} that
         * represents this type server-side
         */
        String customTypeClassName();
    }

    public interface CqlCollection extends CqlType
    {
        CqlFrozen frozen();

        List<CqlType> types();

        CqlField.CqlType type();

        CqlField.CqlType type(int position);

        default boolean isComplex()
        {
            return true;
        }
    }

    public interface CqlMap extends CqlCollection
    {
        CqlField.CqlType keyType();

        CqlField.CqlType valueType();
    }

    public interface CqlSet extends CqlCollection
    {
    }

    public interface CqlList extends CqlCollection
    {
    }

    public interface CqlTuple extends CqlCollection
    {
        ByteBuffer serializeTuple(Object[] values);

        Object[] deserializeTuple(ByteBuffer buffer, boolean isFrozen);
    }

    public interface CqlFrozen extends CqlType
    {
        CqlField.CqlType inner();

        default boolean isFrozen()
        {
            return true;
        }
    }

    public interface CqlUdt extends CqlType
    {
        CqlFrozen frozen();

        String createStatement(CassandraTypes cassandraTypes, String keyspace);

        String keyspace();

        List<CqlField> fields();

        CqlField field(String name);

        CqlField field(int position);

        ByteBuffer serializeUdt(Map<String, Object> values);

        Map<String, Object> deserializeUdt(TypeConverter typeConverter, ByteBuffer buffer, boolean isFrozen);

        @Override
        default boolean isComplex()
        {
            return true;
        }
    }

    public interface CqlUdtBuilder
    {
        CqlUdtBuilder withField(String name, CqlField.CqlType type);

        CqlField.CqlUdt build();
    }

    public enum SortOrder
    {
        ASC,
        DESC
    }

    private final String name;
    private final boolean isPartitionKey;
    private final boolean isClusteringColumn;
    private final boolean isStaticColumn;
    private final CqlType type;
    private final int position;

    public CqlField(boolean isPartitionKey,
                    boolean isClusteringColumn,
                    boolean isStaticColumn,
                    String name,
                    CqlType type,
                    int position)
    {
        Preconditions.checkArgument(!(isPartitionKey && isClusteringColumn),
                                    "Field cannot be both partition key and clustering key");
        Preconditions.checkArgument(!(isPartitionKey && isStaticColumn),
                                    "Field cannot be both partition key and static column");
        Preconditions.checkArgument(!(isClusteringColumn && isStaticColumn),
                                    "Field cannot be both clustering key and static column");
        this.isPartitionKey = isPartitionKey;
        this.isClusteringColumn = isClusteringColumn;
        this.isStaticColumn = isStaticColumn;
        this.name = name;
        this.type = type;
        this.position = position;
    }

    public boolean isPartitionKey()
    {
        return isPartitionKey;
    }

    public boolean isPrimaryKey()
    {
        return isPartitionKey || isClusteringColumn;
    }

    public boolean isClusteringColumn()
    {
        return isClusteringColumn;
    }

    public boolean isStaticColumn()
    {
        return isStaticColumn;
    }

    public boolean isValueColumn()
    {
        return !isPartitionKey && !isClusteringColumn && !isStaticColumn;
    }

    public boolean isNonValueColumn()
    {
        return !isValueColumn();
    }

    public String name()
    {
        return name;
    }

    public CqlType type()
    {
        return type;
    }

    public Object deserializeToType(TypeConverter converter, ByteBuffer buffer)
    {
        return deserializeToType(converter, buffer, false);
    }

    /**
     * Deserialize raw ByteBuffer from Cassandra type and convert to a new type using the TypeConverter.
     *
     * @param converter custom TypeConverter that maps Cassandra type to some other type.
     * @param buffer    raw ByteBuffer
     * @param isFrozen  true if the Cassandra type is frozen
     * @return deserialized object converted to custom type.
     */
    public Object deserializeToType(TypeConverter converter, ByteBuffer buffer, boolean isFrozen)
    {
        return type().deserializeToType(converter, buffer, isFrozen);
    }

    public Object deserializeToJavaType(ByteBuffer buffer)
    {
        return type().deserializeToJavaType(buffer, false);
    }

    /**
     * Deserialize Cassandra raw ByteBuffer and return as standard Java type.
     *
     * @param buffer   raw ByteBuffer
     * @param isFrozen true if the Cassandra type is frozen
     * @return deserialized object as stanard Java type.
     */
    public Object deserializeToJavaType(ByteBuffer buffer, boolean isFrozen)
    {
        return type().deserializeToJavaType(buffer, isFrozen);
    }

    public ByteBuffer serialize(Object value)
    {
        return type.serialize(value);
    }

    public String cqlTypeName()
    {
        return type.cqlName();
    }

    public int position()
    {
        return position;
    }

    @VisibleForTesting
    public CqlField cloneWithPosition(int position)
    {
        return new CqlField(isPartitionKey, isClusteringColumn, isStaticColumn, name, type, position);
    }

    @Override
    public String toString()
    {
        return name + " (" + type + ")";
    }

    @Override
    public int compareTo(@NotNull CqlField that)
    {
        return Integer.compare(this.position, that.position);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, isPartitionKey, isClusteringColumn, isStaticColumn, type, position);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        CqlField that = (CqlField) other;
        return Objects.equals(this.name, that.name)
               && this.isPartitionKey == that.isPartitionKey
               && this.isClusteringColumn == that.isClusteringColumn
               && this.isStaticColumn == that.isStaticColumn
               && Objects.equals(this.type, that.type)
               && this.position == that.position;
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlField>
    {
        private final CassandraTypes cassandraTypes;

        public Serializer(CassandraTypes cassandraTypes)
        {
            this.cassandraTypes = cassandraTypes;
        }

        @Override
        public CqlField read(Kryo kryo, Input input, Class type)
        {
            return new CqlField(input.readBoolean(),
                                input.readBoolean(),
                                input.readBoolean(),
                                input.readString(),
                                CqlType.read(input, cassandraTypes),
                                input.readInt());
        }

        @Override
        public void write(Kryo kryo, Output output, CqlField field)
        {
            output.writeBoolean(field.isPartitionKey());
            output.writeBoolean(field.isClusteringColumn());
            output.writeBoolean(field.isStaticColumn());
            output.writeString(field.name());
            field.type().write(output);
            output.writeInt(field.position());
        }
    }

    public static UnsupportedOperationException notImplemented(CqlType type)
    {
        return notImplemented(type.toString());
    }

    public static UnsupportedOperationException notImplemented(String type)
    {
        return new UnsupportedOperationException(type + " type not implemented");
    }
}
