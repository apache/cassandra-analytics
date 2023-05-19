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

package org.apache.cassandra.db.commitlog;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.CdcRandomAccessReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.exceptions.TransportFailureException;
import org.apache.cassandra.spark.reader.ReaderUtils;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.utils.LoggerHelper;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Copied and refactored from org.apache.cassandra.db.commitlog.CommitLogReader
 * to read from generic source not tied to java.io.File and local file system
 */
@NotThreadSafe
public class BufferingCommitLogReader implements CommitLogReadHandler, AutoCloseable, Comparable<BufferingCommitLogReader>
{
    private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;

    @VisibleForTesting
    public static final int ALL_MUTATIONS = -1;
    private final TableMetadata table;
    private final org.apache.cassandra.spark.cdc.CommitLog log;
    @Nullable
    final CdcOffsetFilter offsetFilter;
    private final CRC32 checksum;
    List<PartitionUpdateWrapper> updates;
    @Nullable
    private final SparkRangeFilter sparkRangeFilter;
    private byte[] buffer;

    private RandomAccessReader reader;
    private CommitLogDescriptor descriptor = null;
    private final ReadStatusTracker statusTracker;
    private int position = 0;

    @NotNull
    private final org.apache.cassandra.spark.cdc.CommitLog.Marker highWaterMark;

    private final LoggerHelper logger;

    @VisibleForTesting
    public BufferingCommitLogReader(@NotNull TableMetadata table,
                                    @NotNull org.apache.cassandra.spark.cdc.CommitLog log,
                                    @NotNull Watermarker watermarker)
    {
        this(table, null, log, null, watermarker.highWaterMark(log.instance()), 0);
    }

    public BufferingCommitLogReader(@NotNull TableMetadata table,
                                    @Nullable CdcOffsetFilter offsetFilter,
                                    @NotNull org.apache.cassandra.spark.cdc.CommitLog log,
                                    @Nullable SparkRangeFilter sparkRangeFilter,
                                    @Nullable org.apache.cassandra.spark.cdc.CommitLog.Marker highWaterMark,
                                    int partitionId)
    {
        this.table = table;
        this.offsetFilter = offsetFilter;
        this.log = log;
        this.updates = new ArrayList<>();
        this.sparkRangeFilter = sparkRangeFilter;
        this.statusTracker = new ReadStatusTracker(ALL_MUTATIONS, false);
        this.checksum = new CRC32();
        this.buffer = new byte[CdcRandomAccessReader.DEFAULT_BUFFER_SIZE];
        this.reader = BufferingCommitLogReader.reader(log);
        this.logger = new LoggerHelper(LoggerFactory.getLogger(BufferingCommitLogReader.class),
                                       "instance", log.instance().nodeName(),
                                       "dc", log.instance().dataCenter(),
                                       "log", log.name(),
                                       "size", log.maxOffset(),
                                       "partitionId", partitionId);

        this.highWaterMark = highWaterMark != null ? highWaterMark : log.zeroMarker();

        try
        {
            readHeader();
            if (skip())
            {
                // If we can skip this CommitLog, close immediately
                close();
            }
            else
            {
                read();
            }
        }
        catch (Throwable throwable)
        {
            close();
            if (isNotFoundError(throwable))
            {
                return;
            }
            logger.warn("Exception reading CommitLog", throwable);
            throw new RuntimeException(throwable);
        }
    }

    public static RandomAccessReader reader(org.apache.cassandra.spark.cdc.CommitLog log)
    {
        return new CdcRandomAccessReader(log);
    }

    private void readHeader() throws IOException
    {
        long startTimeNanos = System.nanoTime();
        try
        {
            descriptor = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
        }
        catch (IOException exception)
        {
            // Let recover deal with it
            logger.warn("IOException reading CommitLog header", exception);
        }
        if (descriptor == null)
        {
            // Don't care about whether or not the handler thinks we can continue. We can't without descriptor.
            // Whether or not we can continue depends on whether this is the last segment.
            handleUnrecoverableError(new CommitLogReadException(
                    String.format("Could not read commit log descriptor in file %s", log.name()),
                    CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                    false));
        }
        else
        {
            logger.info("Read log header", "segmentId", descriptor.id,
                                         "compression", descriptor.compression,
                                             "version", descriptor.version,
                                    "messagingVersion", descriptor.getMessagingVersion(),
                                           "timeNanos", System.nanoTime() - startTimeNanos);
        }
    }

    private void read()
    {
        try
        {
            readCommitLogSegment();
        }
        catch (Throwable throwable)
        {
            if (isNotFoundError(throwable))
            {
                return;
            }
            Throwable cause = ThrowableUtils.rootCause(throwable);
            logger.warn("Exception reading CommitLog", cause);
            throw new RuntimeException(cause);
        }
    }

    private boolean isNotFoundError(Throwable throwable)
    {
        TransportFailureException transportEx = ThrowableUtils.rootCause(throwable, TransportFailureException.class);
        if (transportEx != null && transportEx.isNotFound())
        {
            // Underlying CommitLog may have been removed before/during reading
            // this should only happen when CommitLog is old and can be removed
            logger.warn("CommitLog not found, assuming removed by underlying storage", transportEx);
            return true;
        }
        return false;
    }

    /**
     * Reads mutations from file, handing them off to handler
     *
     * @throws IOException IOException
     */
    private void readCommitLogSegment() throws IOException
    {
        long startTimeNanos = System.nanoTime();
        SeekableCommitLogSegmentReader segmentReader;
        try
        {
            segmentReader = new SeekableCommitLogSegmentReader(this, descriptor, reader, logger, false);
        }
        catch (Exception exception)
        {
            handleUnrecoverableError(new CommitLogReadException(
                    String.format("Unable to create segment reader for commit log file: %s", exception),
                    CommitLogReadErrorReason.UNRECOVERABLE_UNKNOWN_ERROR,
                    false));
            return;
        }

        try
        {
            if (descriptor.id == highWaterMark.segmentId() && reader.getFilePointer() < highWaterMark.position())
            {
                segmentReader.seek(highWaterMark.position());
            }

            for (CommitLogSegmentReader.SyncSegment syncSegment : segmentReader)
            {
                // Only tolerate truncationSerializationHeader if we allow in both global and segment
                // statusTracker.tolerateErrorsInSection = tolerateTruncation && syncSegment.toleratesErrorsInSection;

                statusTracker.errorContext = String.format("Next section at %d in %s",
                                                           syncSegment.fileStartPosition, descriptor.fileName());

                readSection(syncSegment.input, syncSegment.endPosition, descriptor);

                // Track the position at end of previous section after successfully reading mutations,
                // so we can update highwater mark after reading
                position = (int) reader.getFilePointer();

                if (!statusTracker.shouldContinue())
                {
                    break;
                }
            }
        }
        // Unfortunately CommitLogSegmentReader.SegmentIterator (for-loop) cannot throw a checked exception,
        // so we check to see if a RuntimeException is wrapping an IOException
        catch (RuntimeException exception)
        {
            if (exception.getCause() instanceof IOException)
            {
                throw (IOException) exception.getCause();
            }
            throw exception;
        }
        logger.info("Finished reading commit log", "updates", updates.size(),
                                                 "timeNanos", System.nanoTime() - startTimeNanos);
    }

    public boolean skip() throws IOException
    {
        if (shouldSkip(reader))
        {
            logger.info("Skipping playback of empty log");
            return true;
        }

        // Just transform from the file name (no reading of headSkipping playback of empty log:ers) to determine version
        long segmentIdFromFilename = CommitLogDescriptor.fromFileName(log.name()).id;

        if (segmentIdFromFilename != descriptor.id)
        {
            CommitLogReadException readException = new CommitLogReadException(
                    String.format("Segment id mismatch (filename %d, descriptor %d) in file %s",
                                  segmentIdFromFilename, descriptor.id, log.name()),
                    CommitLogReadErrorReason.RECOVERABLE_DESCRIPTOR_ERROR,
                    false);
            return shouldSkipSegmentOnError(readException);
        }

        return shouldSkipSegmentId();
    }

    /**
     * Peek the next 8 bytes to determine if it reaches the end of the file.
     * It should <b>only</b> be called immediately after reading the CommitLog header.
     *
     * @return true to skip; otherwise, return false
     * @throws IOException io exception
     */
    private static boolean shouldSkip(RandomAccessReader reader) throws IOException
    {
        try
        {
            reader.mark();  // Mark position
            int end = reader.readInt();
            long filecrc = reader.readInt() & 0xFFFFFFFFL;
            return end == 0 && filecrc == 0;
        }
        catch (EOFException exception)
        {
            // No data to read
            return true;
        }
        finally
        {
            // Return to marked position before reading mutations
            reader.reset();
        }
    }

    /**
     * Any segment with id >= minPosition.segmentId is a candidate for read
     */
    private boolean shouldSkipSegmentId()
    {
        logger.debug("Reading commit log", "version", descriptor.version,
                                  "messagingVersion", descriptor.getMessagingVersion(),
                                       "compression", descriptor.compression);

        if (highWaterMark.segmentId() > descriptor.id)
        {
            logger.info("Skipping read of fully-flushed log", "segmentId", descriptor.id,
                                                           "minSegmentId", highWaterMark.segmentId());
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Reads a section of a file containing mutations
     *
     * @param reader FileDataInput / logical buffer containing CommitLog mutations
     * @param end    logical numeric end of the segment being read
     * @param desc   Descriptor for CommitLog serialization
     */
    private void readSection(FileDataInput reader, int end, CommitLogDescriptor desc) throws IOException
    {
        while (statusTracker.shouldContinue() && reader.getFilePointer() < end && !reader.isEOF())
        {
            int mutationStart = (int) reader.getFilePointer();
            logger.trace("Reading mutation at", "position", mutationStart);

            long claimedCRC32;
            int serializedSize;
            try
            {
                // We rely on reading serialized size == 0 (LEGACY_END_OF_SEGMENT_MARKER) to identify the end
                // of a segment, which happens naturally due to the 0 padding of the empty segment on creation.
                // However, it's possible with 2.1 era CommitLogs that the last mutation ended less than 4 bytes
                // from the end of the file, which means that we'll be unable to read a full int and instead
                // read an EOF here.
                if (end - reader.getFilePointer() < 4)
                {
                    logger.trace("Not enough bytes left for another mutation in this CommitLog section, continuing");
                    statusTracker.requestTermination();
                    return;
                }

                // Any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == LEGACY_END_OF_SEGMENT_MARKER)
                {
                    logger.trace("Encountered end of segment marker at", "position", reader.getFilePointer());
                    statusTracker.requestTermination();
                    return;
                }

                // Mutation must be at LEAST 10 bytes:
                //   3 for a non-empty Keyspace
                //   3 for a Key (including the 2-byte length from writeUTF/writeWithShortLength)
                //   4 bytes for column count
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                {
                    if (shouldSkipSegmentOnError(new CommitLogReadException(
                            String.format("Invalid mutation size %d at %d in %s",
                                          serializedSize, mutationStart, statusTracker.errorContext),
                            CommitLogReadErrorReason.MUTATION_ERROR,
                            statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }
                    return;
                }

                long claimedSizeChecksum = CommitLogFormat.calculateClaimedChecksum(reader);
                checksum.reset();
                CommitLogFormat.updateChecksum(checksum, serializedSize);

                if (checksum.getValue() != claimedSizeChecksum)
                {
                    if (shouldSkipSegmentOnError(new CommitLogReadException(
                            String.format("Mutation size checksum failure at %d in %s",
                                          mutationStart, statusTracker.errorContext),
                            CommitLogReadErrorReason.MUTATION_ERROR,
                            statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }
                    return;
                }

                if (serializedSize > buffer.length)
                {
                    buffer = new byte[(int) (1.2 * serializedSize)];
                }
                reader.readFully(buffer, 0, serializedSize);

                claimedCRC32 = CommitLogFormat.calculateClaimedCRC32(reader);
            }
            catch (EOFException exception)
            {
                if (shouldSkipSegmentOnError(new CommitLogReadException(
                        String.format("Unexpected end of segment at %d in %s",
                                      mutationStart, statusTracker.errorContext),
                        CommitLogReadErrorReason.EOF,
                        statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }
                return;
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                if (shouldSkipSegmentOnError(new CommitLogReadException(
                        String.format("Mutation checksum failure at %d in %s",
                                      mutationStart, statusTracker.errorContext),
                        CommitLogReadErrorReason.MUTATION_ERROR,
                        statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }
                continue;
            }

            int mutationPosition = (int) reader.getFilePointer();
            readMutationInternal(buffer, serializedSize, mutationPosition, desc);
            statusTracker.addProcessedMutation();
        }
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param inputBuffer      raw byte array w/Mutation data
     * @param size             deserialized size of mutation
     * @param mutationPosition filePointer offset of end of mutation within CommitLogSegment
     * @param descriptor             CommitLogDescriptor being worked on
     */
    @VisibleForTesting
    private void readMutationInternal(byte[] inputBuffer,
                                      int size,
                                      int mutationPosition,
                                      CommitLogDescriptor descriptor) throws IOException
    {
        // For now, we need to go through the motions of deserializing the mutation to determine its size and move
        // the file pointer forward accordingly, even if we're behind the requested minPosition within this SyncSegment

        Mutation mutation;
        try (RebufferingInputStream bufferIn = new DataInputBuffer(inputBuffer, 0, size))
        {
            mutation = Mutation.serializer.deserialize(bufferIn,
                                                       descriptor.getMessagingVersion(),
                                                       DeserializationHelper.Flag.LOCAL);
            // Double-check that what we read is still valid for the current schema
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                update.validate();
            }
        }
        catch (UnknownTableException exception)
        {
            if (exception.id == null)
            {
                return;
            }
            // We see many unknown table exception logs when we skip over mutations from other tables
            logger.trace("Invalid mutation", exception);
            return;
        }
        catch (Throwable throwable)
        {
            JVMStabilityInspector.inspectThrowable(throwable);
            Path path = Files.createTempFile("mutation", "dat");

            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path)))
            {
                out.write(inputBuffer, 0, size);
            }

            // Checksum passed so this error can't be permissible
            handleUnrecoverableError(new CommitLogReadException(
                    String.format("Unexpected error deserializing mutation; saved to %s. "
                                + "This may be caused by replaying a mutation against a table with the same name but incompatible schema. "
                                + "Exception follows: %s", path, throwable),
                    CommitLogReadErrorReason.MUTATION_ERROR,
                    false));
            return;
        }

        logger.trace("Read mutation for", () -> "keyspace", mutation::getKeyspaceName,
                                               () -> "key", mutation::key,
                                          () -> "mutation", () -> mutation.getPartitionUpdates().stream()
                                                                                                .map(AbstractBTreePartition::toString)
                                                                                                .collect(Collectors.joining(", ", "{", "}")));
        handleMutation(mutation, size, mutationPosition, descriptor);
    }

    public boolean isReadable()
    {
        return updates != null;
    }

    public void close()
    {
        if (updates == null)
        {
            return;
        }

        try
        {
            reader.close();
            reader = null;
            updates = null;
        }
        catch (Throwable throwable)
        {
            logger.error("Unexpected exception closing reader", throwable);
        }
    }

    public int compareTo(@NotNull BufferingCommitLogReader that)
    {
        return Long.compare(this.descriptor.id, that.descriptor.id);
    }

    /**
     * Helper methods to deal with changing formats of internals of the CommitLog without polluting deserialization code
     */
    private static class CommitLogFormat
    {
        public static long calculateClaimedChecksum(FileDataInput input) throws IOException
        {
            return input.readInt() & 0xFFFFFFFFL;
        }

        public static void updateChecksum(CRC32 checksum, int serializedSize)
        {
            updateChecksumInt(checksum, serializedSize);
        }

        public static long calculateClaimedCRC32(FileDataInput input) throws IOException
        {
            return input.readInt() & 0xFFFFFFFFL;
        }
    }

    private static class ReadStatusTracker
    {
        private int mutationsLeft;
        public String errorContext = "";         // CHECKSTYLE IGNORE: Public mutable field
        public boolean tolerateErrorsInSection;  // CHECKSTYLE IGNORE: Public mutable field
        private boolean error;

        ReadStatusTracker(int mutationLimit, boolean tolerateErrorsInSection)
        {
            this.error = false;
            this.mutationsLeft = mutationLimit;
            this.tolerateErrorsInSection = tolerateErrorsInSection;
        }

        public void addProcessedMutation()
        {
            if (mutationsLeft == ALL_MUTATIONS)
            {
                return;
            }
            --mutationsLeft;
        }

        public boolean shouldContinue()
        {
            return !error && mutationsLeft != 0;
        }

        public void requestTermination()
        {
            error = true;
        }
    }

    /**
     * @return result object wrapping list of updates buffered and the final highwater marker position
     */
    public Result result()
    {
        return new Result(this);
    }

    public static final class Result
    {
        private final List<PartitionUpdateWrapper> updates;
        private final org.apache.cassandra.spark.cdc.CommitLog.Marker marker;

        private Result(BufferingCommitLogReader reader)
        {
            updates = reader.updates;
            marker = reader.log.markerAt(reader.descriptor.id, reader.position);
        }

        public List<PartitionUpdateWrapper> updates()
        {
            return updates;
        }

        public org.apache.cassandra.spark.cdc.CommitLog.Marker marker()
        {
            return marker;
        }
    }

    // CommitLog Read Handler

    public boolean shouldSkipSegmentOnError(CommitLogReadException exception)
    {
        logger.warn("CommitLog error on shouldSkipSegment", exception);
        return false;
    }

    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
    {
        logger.error("CommitLog unrecoverable error", exception);
        statusTracker.requestTermination();
        throw exception;
    }

    public void handleMutation(Mutation mutation, int size, int mutationPosition, CommitLogDescriptor descriptor)
    {
        mutation.getPartitionUpdates().stream()
                .filter(this::filter)
                .map(update -> Pair.create(update, maxTimestamp(update)))
                .filter(this::withinTimeWindow)
                .map(this::wrapUpdate)
                .forEach(updates::add);
    }

    private long maxTimestamp(PartitionUpdate update)
    {
        // Row deletion
        if (update.rowCount() == 1 && !update.lastRow().deletion().isLive())
        {
            return update.lastRow().deletion().time().markedForDeleteAt();
        }
        else
        {
            return update.maxTimestamp();
        }
    }

    private PartitionUpdateWrapper wrapUpdate(Pair<PartitionUpdate, Long> update)
    {
        return wrapUpdate(update.left, update.right);
    }

    private PartitionUpdateWrapper wrapUpdate(PartitionUpdate update, long maxTimestampMicros)
    {
        return new PartitionUpdateWrapper(table, update, maxTimestampMicros);
    }

    /**
     * @param update the partition update
     * @return true if this is a mutation we are looking for
     */
    private boolean filter(PartitionUpdate update)
    {
        return isTable(update) && withinRange(update);
    }

    private boolean isTable(PartitionUpdate update)
    {
        return update.metadata().keyspace.equals(table.keyspace)
            && update.metadata().name.equals(table.name);
    }

    private boolean withinTimeWindow(Pair<PartitionUpdate, Long> update)
    {
        boolean shouldInclude = withinTimeWindow(update.right);
        if (!shouldInclude)
        {
            logger.info("Exclude the update due to out of the allowed time window.", "update", update.left);
        }
        return shouldInclude;
    }

    private boolean withinTimeWindow(long maxTimestampMicros)
    {
        if (offsetFilter == null)
        {
            return true;
        }
        return offsetFilter.overlaps(maxTimestampMicros);
    }

    /**
     * @param update a CommitLog PartitionUpdate
     * @return true if PartitionUpdate overlaps with the Spark worker token range
     */
    private boolean withinRange(PartitionUpdate update)
    {
        if (sparkRangeFilter == null)
        {
            return true;
        }

        BigInteger token = ReaderUtils.tokenToBigInteger(update.partitionKey().getToken());
        return !sparkRangeFilter.skipPartition(token);
    }
}
