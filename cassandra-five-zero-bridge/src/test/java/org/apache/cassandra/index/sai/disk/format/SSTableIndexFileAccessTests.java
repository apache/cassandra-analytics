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
package org.apache.cassandra.index.sai.disk.format;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SSTableIndexFileAccessTests
{
    private static final String COMPONENT = "na-1-big-SAI+aa+idx+TermsData.db";
    private static final int REMOTE_READ_BUFFER_SIZE = 64 * 1024;

    @BeforeAll
    static void initializeCassandra()
    {
        DatabaseDescriptor.clientInitialization(false);
        Config config = DatabaseDescriptor.getRawConfig();
        config.file_cache_size = new DataStorageSpec.IntMebibytesBound(32);
        config.file_cache_round_up = false;
        config.file_cache_enabled = false;
    }

    @Test
    void testPositionalReadPreservesDestinationLimitAndChannelPosition() throws Exception
    {
        byte[] data = bytes(64);
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, data);

        try (SSTableIndexFileAccess.SSTableFileChannel channel = channel(sstable))
        {
            channel.position(9);
            ByteBuffer destination = ByteBuffer.allocate(16);
            destination.position(3);
            destination.limit(11);

            assertThat(channel.read(destination, 17)).isEqualTo(8);
            assertThat(channel.position()).isEqualTo(9);
            assertThat(destination.position()).isEqualTo(11);
            assertThat(destination.limit()).isEqualTo(11);
            assertThat(Arrays.copyOfRange(destination.array(), 3, 11))
            .containsExactly(Arrays.copyOfRange(data, 17, 25));
        }
    }

    @Test
    void testSequentialReadAdvancesPositionAndHandlesEof() throws Exception
    {
        byte[] data = bytes(10);
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, data);

        try (SSTableIndexFileAccess.SSTableFileChannel channel = channel(sstable))
        {
            channel.position(7);
            ByteBuffer destination = ByteBuffer.allocate(8);

            assertThat(channel.read(destination)).isEqualTo(3);
            assertThat(channel.position()).isEqualTo(10);
            assertThat(Arrays.copyOf(destination.array(), 3)).containsExactly((byte) 7, (byte) 8, (byte) 9);
            assertThat(channel.read(ByteBuffer.allocate(1))).isEqualTo(-1);
            assertThat(channel.read(ByteBuffer.allocate(0))).isZero();
        }
    }

    @Test
    void testPositionalReadStopsAtEofAndRejectsNegativePosition() throws Exception
    {
        byte[] data = bytes(10);
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, data);

        try (SSTableIndexFileAccess.SSTableFileChannel channel = channel(sstable))
        {
            ByteBuffer destination = ByteBuffer.allocate(6);
            assertThat(channel.read(destination, 8)).isEqualTo(2);
            assertThat(destination.position()).isEqualTo(2);
            assertThat(Arrays.copyOf(destination.array(), 2)).containsExactly((byte) 8, (byte) 9);
            assertThat(channel.read(ByteBuffer.allocate(1), data.length)).isEqualTo(-1);
            assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(1), -1))
            .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testScatteringReadHonorsOffsetAndLength() throws Exception
    {
        byte[] data = bytes(32);
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, data);

        try (SSTableIndexFileAccess.SSTableFileChannel channel = channel(sstable))
        {
            channel.position(5);
            ByteBuffer ignored = ByteBuffer.allocate(2);
            ByteBuffer first = ByteBuffer.allocate(3);
            ByteBuffer second = ByteBuffer.allocate(4);
            ByteBuffer[] destinations = {ignored, first, second};

            assertThat(channel.read(destinations, 1, 2)).isEqualTo(7);
            assertThat(channel.position()).isEqualTo(12);
            assertThat(ignored.position()).isZero();
            assertThat(first.array()).containsExactly((byte) 5, (byte) 6, (byte) 7);
            assertThat(second.array()).containsExactly((byte) 8, (byte) 9, (byte) 10, (byte) 11);
        }
    }

    @Test
    void testPositionAndViolatingBounds() throws Exception
    {
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, bytes(16));

        try (SSTableIndexFileAccess.SSTableFileChannel channel = channel(sstable))
        {
            assertThat(channel.size()).isEqualTo(16);
            assertThatThrownBy(() -> channel.position(-1)).isInstanceOf(IllegalArgumentException.class);

            ByteBuffer[] destinations = {ByteBuffer.allocate(1), ByteBuffer.allocate(1)};
            assertThatThrownBy(() -> channel.read(destinations, -1, 1)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> channel.read(destinations, 0, 3)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void testShortBackendReadFailsInsteadOfReturningPartialChunk() throws Exception
    {
        byte[] data = bytes(32);
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, data);
        sstable.shortReads = true;

        try (SSTableIndexFileAccess.SSTableFileChannel channel = channel(sstable))
        {
            assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(8), 4))
            .isInstanceOf(EOFException.class)
            .hasMessageContaining("Short read");
        }
    }

    @Test
    void testClosedChannelRejectsReads() throws Exception
    {
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, bytes(16));
        SSTableIndexFileAccess.SSTableFileChannel channel = channel(sstable);
        channel.close();

        assertThatThrownBy(() -> channel.read(ByteBuffer.allocate(1), 0))
        .isInstanceOf(ClosedChannelException.class);
        assertThatThrownBy(() -> channel.position()).isInstanceOf(ClosedChannelException.class);
    }

    @Test
    void testFileHandleReadsAcrossRemoteChunksAndSupportsBackwardSeek() throws Exception
    {
        byte[] data = bytes(REMOTE_READ_BUFFER_SIZE * 3 + 137);
        TestInMemorySSTable sstable = new TestInMemorySSTable(COMPONENT, data);
        SSTableIndexFileAccess access = new SSTableIndexFileAccess(sstable);
        File file = new File("build/data/" + COMPONENT);
        assertThat(file.exists()).isFalse();

        try (FileHandle handle = access.open(file);
             RandomAccessReader reader = handle.createReader())
        {
            assertRead(reader, data, REMOTE_READ_BUFFER_SIZE - 11, 64);
            assertRead(reader, data, 2L * REMOTE_READ_BUFFER_SIZE + 7, 97);
            assertRead(reader, data, 23, 41); // seek backwards into the first chunk
            assertRead(reader, data, data.length - 19L, 19);
            assertThat(reader.read()).isEqualTo(-1);
        }

        assertThat(sstable.reads).hasSizeGreaterThanOrEqualTo(3);
        assertThat(sstable.reads)
        .allSatisfy(read -> assertThat(read.requested).isLessThanOrEqualTo(REMOTE_READ_BUFFER_SIZE));
        assertThat(sstable.reads.stream().map(read -> read.position / REMOTE_READ_BUFFER_SIZE).distinct().count())
        .isGreaterThanOrEqualTo(3);
    }

    private static void assertRead(RandomAccessReader reader,
                                   byte[] expected,
                                   long position,
                                   int length) throws IOException
    {
        byte[] actual = new byte[length];
        reader.seek(position);
        reader.readFully(actual);
        assertThat(actual).containsExactly(Arrays.copyOfRange(expected, (int) position, (int) position + length));
    }

    private static SSTableIndexFileAccess.SSTableFileChannel channel(TestInMemorySSTable sstable)
    {
        return new SSTableIndexFileAccess.SSTableFileChannel(sstable, COMPONENT, sstable.data.length);
    }

    private static byte[] bytes(int length)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) i;
        }
        return data;
    }

    private static final class Read
    {
        private final long position;
        private final int requested;

        private Read(long position, int requested)
        {
            this.position = position;
            this.requested = requested;
        }
    }

    private static final class TestInMemorySSTable extends SSTable
    {
        private static final long serialVersionUID = 1L;

        private final String componentName;
        private final byte[] data;
        private final List<Read> reads = new ArrayList<>();
        private boolean shortReads;

        private TestInMemorySSTable(String componentName, byte[] data)
        {
            this.componentName = componentName;
            this.data = data;
        }

        @Override
        @Nullable
        protected InputStream openInputStream(FileType fileType)
        {
            return null;
        }

        @Override
        public long length(FileType fileType)
        {
            return 0;
        }

        @Override
        public boolean isMissing(FileType fileType)
        {
            return true;
        }

        @Override
        @NotNull
        public Set<String> customComponentNames()
        {
            return Collections.singleton(componentName);
        }

        @Override
        public long customComponentLength(@NotNull String name)
        {
            if (!componentName.equals(name))
            {
                throw new IllegalArgumentException("Unknown component: " + name);
            }
            return data.length;
        }

        @Override
        public int readCustomComponent(@NotNull String name,
                                       long position,
                                       @NotNull ByteBuffer destination)
        {
            if (!componentName.equals(name))
            {
                throw new IllegalArgumentException("Unknown component: " + name);
            }
            if (position >= data.length)
            {
                return -1;
            }

            int requested = Math.min(destination.remaining(), data.length - (int) position);
            reads.add(new Read(position, requested));
            int count = shortReads && requested > 1 ? requested - 1 : requested;
            destination.put(data, (int) position, count);
            return count;
        }

        @Override
        public String getDataFileName()
        {
            return "na-1-big-Data.db";
        }

        @Override
        public boolean equals(Object other)
        {
            return this == other;
        }

        @Override
        public int hashCode()
        {
            return System.identityHashCode(this);
        }
    }
}
