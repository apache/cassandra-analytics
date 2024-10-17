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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TokenUtils;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

@SuppressWarnings("WeakerAccess")
public final class ReaderUtils extends TokenUtils
{
    private static final int CHECKSUM_LENGTH = 4;  // CRC32
    private static final Constructor<?> SERIALIZATION_HEADER =
    Arrays.stream(SerializationHeader.Component.class.getDeclaredConstructors())
          .filter(constructor -> constructor.getParameterCount() == 5)
          .findFirst()
          .orElseThrow(() -> new RuntimeException("Could not find SerializationHeader.Component constructor"));
    public static final ByteBuffer SUPER_COLUMN_MAP_COLUMN = ByteBufferUtil.EMPTY_BYTE_BUFFER;

    static
    {
        SERIALIZATION_HEADER.setAccessible(true);
    }

    private ReaderUtils()
    {
        super();
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    static ByteBuffer encodeCellName(TableMetadata metadata,
                                     ClusteringPrefix clustering,
                                     ByteBuffer columnName,
                                     ByteBuffer collectionElement)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;

        if (!TableMetadata.Flag.isCompound(metadata.flags))
        {
            if (isStatic)
            {
                return columnName;
            }

            assert clustering.size() == 1 : "Expected clustering size to be 1, but was " + clustering.size();
            return clustering.bufferAt(0);
        }

        // We use comparator.size() rather than clustering.size() because of static clusterings
        int clusteringSize = metadata.comparator.size();
        int size = clusteringSize + (TableMetadata.Flag.isDense(metadata.flags) ? 0 : 1)
                   + (collectionElement == null ? 0 : 1);
        if (TableMetadata.Flag.isSuper(metadata.flags))
        {
            size = clusteringSize + 1;
        }

        ByteBuffer[] values = new ByteBuffer[size];
        for (int index = 0; index < clusteringSize; index++)
        {
            if (isStatic)
            {
                values[index] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                continue;
            }

            ByteBuffer value = clustering.bufferAt(index);
            // We can have null (only for dense compound tables for backward compatibility reasons),
            // but that means we're done and should stop there as far as building the composite is concerned
            if (value == null)
            {
                return CompositeType.build(ByteBufferAccessor.instance, Arrays.copyOfRange(values, 0, index));
            }

            values[index] = value;
        }

        if (TableMetadata.Flag.isSuper(metadata.flags))
        {
            // We need to set the "column" (in thrift terms) name, i.e. the value corresponding to the subcomparator.
            // What it is depends on whether this is a cell for a declared "static" column
            // or a "dynamic" column part of the super-column internal map.
            assert columnName != null;  // This should never be null for supercolumns, see decodeForSuperColumn() above
            values[clusteringSize] = columnName.equals(SUPER_COLUMN_MAP_COLUMN)
                                     ? collectionElement
                                     : columnName;
        }
        else
        {
            if (!TableMetadata.Flag.isDense(metadata.flags))
            {
                values[clusteringSize] = columnName;
            }
            if (collectionElement != null)
            {
                values[clusteringSize + 1] = collectionElement;
            }
        }

        return CompositeType.build(ByteBufferAccessor.instance, isStatic, values);
    }

    public static Pair<DecoratedKey, DecoratedKey> keysFromIndex(@NotNull TableMetadata metadata,
                                                                 @NotNull SSTable ssTable) throws IOException
    {
        try (InputStream primaryIndex = ssTable.openPrimaryIndexStream())
        {
            if (primaryIndex != null)
            {
                IPartitioner partitioner = metadata.partitioner;
                Pair<ByteBuffer, ByteBuffer> keys = readPrimaryIndex(primaryIndex, true, Collections.emptyList());
                return Pair.create(partitioner.decorateKey(keys.left), partitioner.decorateKey(keys.right));
            }
        }
        return Pair.create(null, null);
    }

    static boolean anyFilterKeyInIndex(@NotNull SSTable ssTable,
                                       @NotNull List<PartitionKeyFilter> filters) throws IOException
    {
        if (filters.isEmpty())
        {
            return false;
        }

        try (InputStream primaryIndex = ssTable.openPrimaryIndexStream())
        {
            if (primaryIndex != null)
            {
                Pair<ByteBuffer, ByteBuffer> keys = readPrimaryIndex(primaryIndex, false, filters);
                if (keys.left != null || keys.right != null)
                {
                    return false;
                }
            }
        }
        return true;
    }

    static Map<MetadataType, MetadataComponent> deserializeStatsMetadata(SSTable ssTable,
                                                                         Descriptor descriptor) throws IOException
    {
        try (InputStream statsStream = ssTable.openStatsStream())
        {
            return deserializeStatsMetadata(statsStream,
                                            EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER),
                                            descriptor);
        }
    }

    /**
     * Deserialize Statistics.db file to pull out metadata components needed for SSTable deserialization
     *
     * @param is            input stream for Statistics.db file
     * @param selectedTypes enum of MetadataType to deserialize
     * @param descriptor    SSTable file descriptor
     * @return map of MetadataComponent for each requested MetadataType
     * @throws IOException
     */
    static Map<MetadataType, MetadataComponent> deserializeStatsMetadata(InputStream is,
                                                                         EnumSet<MetadataType> selectedTypes,
                                                                         Descriptor descriptor) throws IOException
    {
        DataInputStream in = new DataInputPlus.DataInputStreamPlus(is);
        boolean isChecksummed = descriptor.version.hasMetadataChecksum();
        CRC32 crc = new CRC32();

        int count = in.readInt();
        updateChecksumInt(crc, count);
        maybeValidateChecksum(crc, in, descriptor);

        int[] ordinals = new int[count];
        int[] offsets = new int[count];
        int[] lengths = new int[count];

        for (int index = 0; index < count; index++)
        {
            ordinals[index] = in.readInt();
            updateChecksumInt(crc, ordinals[index]);

            offsets[index] = in.readInt();
            updateChecksumInt(crc, offsets[index]);
        }
        maybeValidateChecksum(crc, in, descriptor);

        for (int index = 0; index < count - 1; index++)
        {
            lengths[index] = offsets[index + 1] - offsets[index];
        }

        MetadataType[] allMetadataTypes = MetadataType.values();
        Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);
        for (int index = 0; index < count - 1; index++)
        {
            MetadataType type = allMetadataTypes[ordinals[index]];

            if (!selectedTypes.contains(type))
            {
                in.skipBytes(lengths[index]);
                continue;
            }

            byte[] bytes = new byte[isChecksummed ? lengths[index] - CHECKSUM_LENGTH : lengths[index]];
            in.readFully(bytes);

            crc.reset();
            crc.update(bytes);
            maybeValidateChecksum(crc, in, descriptor);

            components.put(type, deserializeMetadataComponent(descriptor.version, bytes, type));
        }

        MetadataType type = allMetadataTypes[ordinals[count - 1]];
        if (!selectedTypes.contains(type))
        {
            return components;
        }

        // We do not have in.bytesRemaining() (as in FileDataInput),
        // so need to read remaining bytes to get final component
        byte[] remainingBytes = ByteBufferUtils.readRemainingBytes(in, 256);
        byte[] bytes;
        if (descriptor.version.hasMetadataChecksum())
        {
            ByteBuffer buffer = ByteBuffer.wrap(remainingBytes);
            int length = buffer.remaining() - 4;
            bytes = new byte[length];
            buffer.get(bytes, 0, length);
            crc.reset();
            crc.update(bytes);
            validateChecksum(crc, buffer.getInt(), descriptor);
        }
        else
        {
            bytes = remainingBytes;
        }

        components.put(type, deserializeMetadataComponent(descriptor.version, bytes, type));

        return components;
    }

    private static void maybeValidateChecksum(CRC32 crc, DataInputStream in, Descriptor descriptor) throws IOException
    {
        if (descriptor.version.hasMetadataChecksum())
        {
            validateChecksum(crc, in.readInt(), descriptor);
        }
    }

    private static void validateChecksum(CRC32 crc, int expectedChecksum, Descriptor descriptor)
    {
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum)
        {
            String filename = descriptor.filenameFor(Component.STATS);
            throw new CorruptSSTableException(new IOException("Checksums do not match for " + filename), filename);
        }
    }

    private static MetadataComponent deserializeValidationMetaData(@NotNull DataInputBuffer in) throws IOException
    {
        return new ValidationMetadata(in.readUTF(), in.readDouble());
    }

    private static MetadataComponent deserializeMetadataComponent(@NotNull Version version,
                                                                  @NotNull byte[] buffer,
                                                                  @NotNull MetadataType type) throws IOException
    {
        DataInputBuffer in = new DataInputBuffer(buffer);
        if (type == MetadataType.HEADER)
        {
            return deserializeSerializationHeader(in);
        }
        else if (type == MetadataType.VALIDATION)
        {
            return deserializeValidationMetaData(in);
        }
        return type.serializer.deserialize(version, in);
    }

    private static MetadataComponent deserializeSerializationHeader(@NotNull DataInputBuffer in) throws IOException
    {
        // We need to deserialize data type class names using shaded package names
        EncodingStats stats = EncodingStats.serializer.deserialize(in);
        AbstractType<?> keyType = readType(in);
        int size = (int) in.readUnsignedVInt();
        List<AbstractType<?>> clusteringTypes = new ArrayList<>(size);

        for (int index = 0; index < size; ++index)
        {
            clusteringTypes.add(readType(in));
        }

        Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap<>();
        Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap<>();
        readColumnsWithType(in, staticColumns);
        readColumnsWithType(in, regularColumns);

        try
        {
            // TODO: We should expose this code in Cassandra to make it easier to do this with unit tests in Cassandra
            return (SerializationHeader.Component) SERIALIZATION_HEADER.newInstance(keyType,
                                                                                    clusteringTypes,
                                                                                    staticColumns,
                                                                                    regularColumns,
                                                                                    stats);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    private static void readColumnsWithType(@NotNull DataInputPlus in,
                                            @NotNull Map<ByteBuffer, AbstractType<?>> typeMap) throws IOException
    {
        int length = (int) in.readUnsignedVInt();
        for (int index = 0; index < length; index++)
        {
            ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
            typeMap.put(name, readType(in));
        }
    }

    private static AbstractType<?> readType(@NotNull DataInputPlus in) throws IOException
    {
        return TypeParser.parse(UTF8Type.instance.compose(ByteBufferUtil.readWithVIntLength(in)));
    }

    /**
     * Read primary Index.db file, read through all partitions to get first and last partition key
     *
     * @param primaryIndex input stream for Index.db file
     * @return pair of first and last decorated keys
     * @throws IOException
     */
    @SuppressWarnings("InfiniteLoopStatement")
    static Pair<ByteBuffer, ByteBuffer> readPrimaryIndex(@NotNull InputStream primaryIndex,
                                                         boolean readFirstLastKey,
                                                         @NotNull List<PartitionKeyFilter> filters) throws IOException
    {
        ByteBuffer firstKey = null;
        ByteBuffer lastKey = null;
        try (DataInputStream dis = new DataInputStream(primaryIndex))
        {
            byte[] last = null;
            try
            {
                while (true)
                {
                    int length = dis.readUnsignedShort();
                    byte[] buffer = new byte[length];
                    dis.readFully(buffer);
                    if (firstKey == null)
                    {
                        firstKey = ByteBuffer.wrap(buffer);
                    }
                    last = buffer;
                    ByteBuffer key = ByteBuffer.wrap(last);
                    if (!readFirstLastKey && filters.stream().anyMatch(filter -> filter.filter(key)))
                    {
                        return Pair.create(null, null);
                    }

                    // Read position and skip promoted index
                    skipRowIndexEntry(dis);
                }
            }
            catch (EOFException ignored)
            {
            }

            if (last != null)
            {
                lastKey = ByteBuffer.wrap(last);
            }
        }

        return Pair.create(firstKey, lastKey);
    }

    static void skipRowIndexEntry(DataInputStream dis) throws IOException
    {
        readPosition(dis);
        skipPromotedIndex(dis);
    }

    static int vIntSize(long value)
    {
        return VIntCoding.computeUnsignedVIntSize(value);
    }

    static void writePosition(long value, ByteBuffer buffer)
    {
        VIntCoding.writeUnsignedVInt(value, buffer);
    }

    static long readPosition(DataInputStream dis) throws IOException
    {
        return VIntCoding.readUnsignedVInt(dis);
    }

    /**
     * @return the total bytes skipped
     */
    public static int skipPromotedIndex(DataInputStream dis) throws IOException
    {
        final long val = VIntCoding.readUnsignedVInt(dis);
        final int size = (int) val;
        if (size > 0)
        {
            ByteBufferUtils.skipBytesFully(dis, size);
        }
        return Math.max(size, 0) + VIntCoding.computeUnsignedVIntSize(val);
    }

    static List<PartitionKeyFilter> filterKeyInBloomFilter(
    @NotNull SSTable ssTable,
    @NotNull IPartitioner partitioner,
    Descriptor descriptor,
    @NotNull List<PartitionKeyFilter> partitionKeyFilters) throws IOException
    {
        try
        {
            BloomFilter bloomFilter = SSTableCache.INSTANCE.bloomFilter(ssTable, descriptor);
            return partitionKeyFilters.stream()
                                      .filter(filter -> bloomFilter.isPresent(partitioner.decorateKey(filter.key())))
                                      .collect(Collectors.toList());
        }
        catch (Exception exception)
        {
            if (exception instanceof FileNotFoundException)
            {
                return partitionKeyFilters;
            }
            throw exception;
        }
    }

    static BloomFilter readFilter(@NotNull SSTable ssTable, boolean hasOldBfFormat) throws IOException
    {
        try (InputStream filterStream = ssTable.openFilterStream())
        {
            if (filterStream != null)
            {
                try (DataInputStream dis = new DataInputStream(filterStream))
                {
                    return BloomFilterSerializer.deserialize(dis, hasOldBfFormat);
                }
            }
        }
        throw new FileNotFoundException();
    }
}
