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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.api.CommitLog;
import org.apache.cassandra.cdc.api.CommitLogMarkers;
import org.apache.cassandra.cdc.api.Marker;
import org.apache.cassandra.cdc.stats.ICdcStats;
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
import org.apache.cassandra.spark.exceptions.TransportFailureException;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.LoggerHelper;
import org.apache.cassandra.spark.utils.Pair;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.TokenUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Reworked implementation of `org.apache.cassandra.db.commitlog.CommitLogReader` to read from generic source not tied
 * to java.io.File and local file system and buffer all filtered mutations in memory for consumption.
 */
@NotThreadSafe
public class BufferingCommitLogReader implements CommitLogReadHandler,
                                                 AutoCloseable,
                                                 Comparable<BufferingCommitLogReader>,
                                                 org.apache.cassandra.cdc.api.CommitLogReader
{
    private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;

    @VisibleForTesting
    public static final int ALL_MUTATIONS = -1;
    private final CommitLog log;
    private final CRC32 checksum;
    @Nullable
    private final TokenRange tokenRange;
    private final ReadStatusTracker statusTracker;
    private int position = 0;
    private final ICdcStats stats;
    private final long segmentId;
    private final int messagingVersion;
    @NotNull
    private final Marker startMarker;
    private final LoggerHelper logger;
    @Nullable
    private final AsyncExecutor executor;
    @Nullable
    private final Consumer<Marker> listener;
    @NotNull
    private final CommitLogMarkers markers;
    @Nullable
    private final Long startTimestampMicros;

    // mutable state
    private byte[] buffer;
    List<PartitionUpdateWrapper> updates = new ArrayList<>(1024);
    private RandomAccessReader reader;
    private CommitLogDescriptor desc = null;
    private boolean skipped = false;

    @VisibleForTesting
    public BufferingCommitLogReader(@NotNull CommitLog log,
                                    @Nullable Marker startMarker,
                                    @NotNull ICdcStats stats,
                                    @Nullable Consumer<Marker> listener)
    {
        this(log, null, CommitLogMarkers.of(startMarker), 0, stats, null, listener, null, false);
    }

    public BufferingCommitLogReader(@NotNull CommitLog log,
                                    @Nullable TokenRange tokenRange,
                                    @NotNull CommitLogMarkers markers,
                                    int partitionId,
                                    @NotNull ICdcStats stats,
                                    @Nullable AsyncExecutor executor,
                                    @Nullable Consumer<Marker> listener,
                                    @Nullable Long startTimestampMicros,
                                    boolean readHeader)
    {
        this.log = log;
        this.tokenRange = tokenRange;
        this.statusTracker = new ReadStatusTracker(ALL_MUTATIONS, false);
        this.checksum = new CRC32();
        this.buffer = new byte[CdcRandomAccessReader.DEFAULT_BUFFER_SIZE];
        this.reader = BufferingCommitLogReader.reader(log);
        this.markers = markers;
        this.listener = listener;
        this.startTimestampMicros = startTimestampMicros;

        Pair<Integer, Long> pair = CommitLog.extractVersionAndSegmentId(log)
                                            .orElseThrow(() -> new IllegalStateException("Could not extract segmentId from CommitLog filename"));
        this.messagingVersion = pair.getLeft();
        this.segmentId = pair.getRight();
        this.logger = new LoggerHelper(LoggerFactory.getLogger(BufferingCommitLogReader.class),
                                       "instance", log.instance().nodeName(),
                                       "dc", log.instance().dataCenter(),
                                       "log", log.name(),
                                       "size", log.maxOffset(),
                                       "segmentId", this.segmentId,
                                       "partitionId", partitionId);

        Marker startMarker = markers.startMarker(log);
        this.startMarker = startMarker.segmentId() == segmentId ? startMarker : log.zeroMarker();
        this.stats = stats;
        this.executor = executor;

        logger.trace("Opening BufferingCommitLogReader");
        try
        {
            if (readHeader || this.startMarker.position() == 0)
            {
                this.readHeader();
                if (skip(this.startMarker))
                {
                    // if we can skip this CommitLog, close immediately
                    logger.trace("Skipping commit log after reading header");
                    stats.skippedCommitLogsCount(1);
                    skipped = true;
                    return;
                }
            }
            else if (shouldSkipSegmentId(this.startMarker))
            {
                logger.trace("Skipping log");
                stats.skippedCommitLogsCount(1);
                skipped = true;
                return;
            }
            read();
        }
        catch (Throwable t)
        {
            skipped = true;
            if (isNotFoundError(t))
            {
                return;
            }
            logger.warn("Exception reading CommitLog", t);
            throw new RuntimeException(t);
        }
    }

    public static RandomAccessReader reader(CommitLog log)
    {
        return new CdcRandomAccessReader(log);
    }

    private void readHeader() throws IOException
    {
        logger.trace("Reading header");
        long startTimeNanos = System.nanoTime();
        try
        {
            desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
        }
        catch (IOException e)
        {
            // let recover deal with it
            logger.warn("IOException reading CommitLog header", e);
            stats.commitLogHeaderReadFailureCount(1);
        }
        if (desc == null)
        {
            // don't care about whether or not the handler thinks we can continue. We can't w/out descriptor.
            // whether or not we can continue depends on whether this is the last segment
            this.handleUnrecoverableError(
            new CommitLogReadException(String.format("Could not read commit log descriptor in file %s", log.name()),
                                       CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                       false));
        }
        else
        {
            long timeTakenToReadHeader = System.nanoTime() - startTimeNanos;
            logger.debug("Read log header", "segmentId", desc.id, "compression", desc.compression,
                         "version", desc.version, "messagingVersion", desc.getMessagingVersion(),
                         "timeNanos", timeTakenToReadHeader);
            stats.commitLogHeaderReadTime(timeTakenToReadHeader);
        }
    }

    private void read()
    {
        try
        {
            readCommitLogSegment();
        }
        catch (Throwable t)
        {
            if (isNotFoundError(t))
            {
                return;
            }
            Throwable cause = ThrowableUtils.rootCause(t);
            logger.warn("Exception reading CommitLog", cause);
            throw new RuntimeException(cause);
        }
    }

    private boolean isNotFoundError(Throwable t)
    {
        TransportFailureException transportEx = ThrowableUtils.rootCause(t, TransportFailureException.class);
        if (transportEx != null && transportEx.isNotFound())
        {
            // underlying commit log may have been removed before/during reading
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
            segmentReader = new SeekableCommitLogSegmentReader(segmentId, this, desc, reader, logger, false);
        }
        catch (Exception e)
        {
            this.handleUnrecoverableError(new CommitLogReadException(
            String.format("Unable to create segment reader for commit log file: %s", e),
            CommitLogReadErrorReason.UNRECOVERABLE_UNKNOWN_ERROR,
            false));
            return;
        }

        try
        {
            if (reader.getFilePointer() < startMarker.position())
            {
                stats.commitLogBytesSkippedOnRead(startMarker.position() - reader.getFilePointer());
                segmentReader.seek(startMarker.position());
            }

            for (CommitLogSegmentReader.SyncSegment syncSegment : segmentReader)
            {
                // Only tolerate truncationSerializationHeader if we allow in both global and segment
//                statusTracker.tolerateErrorsInSection = tolerateTruncation && syncSegment.toleratesErrorsInSection;

                statusTracker.errorContext = String.format("Next section at %d in %s", syncSegment.fileStartPosition, log.name());

                readSection(syncSegment.input, syncSegment.endPosition);

                // track the position at end of previous section after successfully reading mutations
                // so we can update highwater mark after reading
                this.position = (int) reader.getFilePointer();

                if (listener != null)
                {
                    listener.accept(log.markerAt(segmentId, position));
                }

                if (!statusTracker.shouldContinue())
                {
                    break;
                }
            }
        }
        // Unfortunately CommitLogSegmentReader.SegmentIterator (for-loop) cannot throw a checked exception,
        // so we check to see if a RuntimeException is wrapping an IOException.
        catch (RuntimeException re)
        {
            if (re.getCause() instanceof IOException)
            {
                throw (IOException) re.getCause();
            }
            throw re;
        }
        logger.debug("Finished reading commit log", "updates", updates.size(), "timeNanos", (System.nanoTime() - startTimeNanos));
    }

    public boolean skip(@Nullable Marker highWaterMark) throws IOException
    {
        if (shouldSkip(reader))
        {
            logger.debug("Skipping playback of empty log");
            return true;
        }

        // just transform from the file name (no reading of headers) to determine version
        long segmentIdFromFilename = CommitLogDescriptor.fromFileName(log.name()).id;

        if (segmentIdFromFilename != desc.id)
        {
            CommitLogReadException readException = new CommitLogReadException(
            String.format("Segment id mismatch (filename %d, descriptor %d) in file %s", segmentIdFromFilename, desc.id, log.name()),
            CommitLogReadErrorReason.RECOVERABLE_DESCRIPTOR_ERROR,
            false);
            return this.shouldSkipSegmentOnError(readException);
        }

        return shouldSkipSegmentId(highWaterMark);
    }

    /**
     * Peek the next 8 bytes to determine if it reaches the end of the file.
     * It should _only_ be called immediately after reading the commit log header.
     *
     * @return true to skip; otherwise, return false.
     * @throws IOException io exception
     */
    private static boolean shouldSkip(RandomAccessReader reader) throws IOException
    {
        try
        {
            reader.mark(); // mark position
            int end = reader.readInt();
            long filecrc = reader.readInt() & 0xffffffffL;
            return end == 0 && filecrc == 0;
        }
        catch (EOFException e)
        {
            // no data to read
            return true;
        }
        finally
        {
            // return to marked position before reading mutations
            reader.reset();
        }
    }

    /**
     * Any segment with id >= minPosition.segmentId is a candidate for read.
     */
    private boolean shouldSkipSegmentId(@Nullable Marker highWaterMark)
    {
        logger.debug("Reading commit log", "version", messagingVersion, "compression", desc != null ? desc.compression : "disabled");

        if (highWaterMark != null && highWaterMark.segmentId() > segmentId)
        {
            logger.info("Skipping read of fully-flushed log", "segmentId", segmentId, "minSegmentId", highWaterMark.segmentId());
            return true;
        }
        return false;
    }

    /**
     * Reads a section of a file containing mutations
     *
     * @param reader FileDataInput / logical buffer containing commitlog mutations
     * @param end    logical numeric end of the segment being read
     */
    private void readSection(FileDataInput reader,
                             int end) throws IOException
    {
        long startTimeNanos = System.nanoTime();
        logger.trace("Reading log section", "end", end);

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
                // However, it's possible with 2.1 era commitlogs that the last mutation ended less than 4 bytes
                // from the end of the file, which means that we'll be unable to read an a full int and instead
                // read an EOF here
                if (end - reader.getFilePointer() < 4)
                {
                    logger.trace("Not enough bytes left for another mutation in this CommitLog section, continuing");
                    statusTracker.requestTermination();
                    return;
                }

                // any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == LEGACY_END_OF_SEGMENT_MARKER)
                {
                    logger.trace("Encountered end of segment marker at", "position", reader.getFilePointer());
                    statusTracker.requestTermination();
                    return;
                }

                // Mutation must be at LEAST 10 bytes:
                //    3 for a non-empty Keyspace
                //    3 for a Key (including the 2-byte length from writeUTF/writeWithShortLength)
                //    4 bytes for column count.
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                {
                    if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                    String.format("Invalid mutation size %d at %d in %s", serializedSize, mutationStart, statusTracker.errorContext),
                    CommitLogReadErrorReason.MUTATION_ERROR,
                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }

                    stats.commitLogInvalidSizeMutationCount(1);
                    return;
                }

                long claimedSizeChecksum = CommitLogFormat.calculateClaimedChecksum(reader);
                checksum.reset();
                CommitLogFormat.updateChecksum(checksum, serializedSize);

                if (checksum.getValue() != claimedSizeChecksum)
                {
                    if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                    String.format("Mutation size checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                    CommitLogReadErrorReason.MUTATION_ERROR,
                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }

                    stats.mutationsChecksumMismatchCount(1);
                    return;
                }

                if (serializedSize > buffer.length)
                {
                    buffer = new byte[(int) (1.2 * serializedSize)];
                }
                reader.readFully(buffer, 0, serializedSize);

                claimedCRC32 = CommitLogFormat.calculateClaimedCRC32(reader);
            }
            catch (EOFException eof)
            {
                if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                String.format("Unexpected end of segment at %d in %s", mutationStart, statusTracker.errorContext),
                CommitLogReadErrorReason.EOF,
                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }

                stats.commitLogSegmentUnexpectedEndErrorCount(1);
                return;
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                String.format("Mutation checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                CommitLogReadErrorReason.MUTATION_ERROR,
                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }

                stats.mutationsChecksumMismatchCount(1);
                continue;
            }

            int mutationPosition = (int) reader.getFilePointer();
            readMutationInternal(buffer, serializedSize, mutationPosition);
            statusTracker.addProcessedMutation();
        }

        stats.commitLogSegmentReadTime(System.nanoTime() - startTimeNanos);
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param inputBuffer      raw byte array w/Mutation data
     * @param size             deserialized size of mutation
     * @param mutationPosition filePointer offset of end of mutation within CommitLogSegment
     */
    @VisibleForTesting
    private void readMutationInternal(byte[] inputBuffer,
                                      int size,
                                      int mutationPosition) throws IOException
    {
        // For now, we need to go through the motions of deserializing the mutation to determine its size and move
        // the file pointer forward accordingly, even if we're behind the requested minPosition within this SyncSegment.

        Mutation mutation;
        try (RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size))
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                       messagingVersion,
                                                       DeserializationHelper.Flag.LOCAL);
        }
        catch (UnknownTableException ex)
        {
            if (ex.id == null)
            {
                return;
            }
            logger.trace("Invalid mutation", ex); // we see many unknown table exception logs when we skip over mutations from other tables
            stats.mutationsIgnoredUnknownTableCount(1);

            return;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            Path p = Files.createTempFile("mutation", "dat");

            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(p)))
            {
                out.write(inputBuffer, 0, size);
            }

            // Checksum passed so this error can't be permissible.
            this.handleUnrecoverableError(new CommitLogReadException(
            String.format(
            "Unexpected error deserializing mutation; saved to %s.  " +
            "This may be caused by replaying a mutation against a table with the same name but incompatible schema.  " +
            "Exception follows: %s", p, t),
            CommitLogReadErrorReason.MUTATION_ERROR,
            false));

            stats.mutationsDeserializeFailedCount(1);

            return;
        }

        //  Cassandra has a bug of getting the string representation of tombstoned cell of some type (date, smallint, etc.)
        //  in a multi-cell collection. The trace logging below triggers AbstractCell#toString()
        // JIRA: CASSANDRA-17695 AbstractCell#toString throws MarshalException for cell in collection
        logger.trace("Read mutation for", () -> "keyspace", mutation::getKeyspaceName, () -> "key", mutation::key,
                     () -> "mutation", () -> '{'
                                             + mutation.getPartitionUpdates().stream()
                                                       .map(AbstractBTreePartition::toString)
                                                       .collect(Collectors.joining(", "))
                                             + '}');

        stats.mutationsReadCount(1);
        stats.mutationsReadBytes(size);

        this.handleMutation(mutation, size, mutationPosition, desc);
    }

    public void close()
    {
        if (updates == null)
        {
            return;
        }

        try
        {
            logger.trace("Closing log");
            reader.close();
            reader = null;
            updates = null;
        }
        catch (Throwable t)
        {
            logger.error("Unexpected exception closing reader", t);
        }
    }

    public int compareTo(@NotNull BufferingCommitLogReader o)
    {
        return Long.compare(segmentId, o.segmentId);
    }

    public List<PartitionUpdateWrapper> updates()
    {
        return updates;
    }

    public CommitLog log()
    {
        return log;
    }

    public long segmentId()
    {
        return segmentId;
    }

    public int position()
    {
        return position;
    }

    public boolean skipped()
    {
        return skipped;
    }

    /**
     * Helper methods to deal with changing formats of internals of the CommitLog without polluting deserialization code.
     */
    private static class CommitLogFormat
    {
        public static long calculateClaimedChecksum(FileDataInput input) throws IOException
        {
            return input.readInt() & 0xffffffffL;
        }

        public static void updateChecksum(CRC32 checksum, int serializedSize)
        {
            updateChecksumInt(checksum, serializedSize);
        }

        public static long calculateClaimedCRC32(FileDataInput input) throws IOException
        {
            return input.readInt() & 0xffffffffL;
        }
    }

    private static class ReadStatusTracker
    {
        private int mutationsLeft;
        public String errorContext = "";
        public boolean tolerateErrorsInSection;
        private boolean error;

        private ReadStatusTracker(int mutationLimit, boolean tolerateErrorsInSection)
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
     * @return result object wrapping list of updates buffered and the final highwater marker position.
     */
    public Result result()
    {
        return new Result(this);
    }

    // CommitLogReadHandler

    public boolean shouldSkipSegmentOnError(CommitLogReadException e)
    {
        logger.warn("CommitLog error on shouldSkipSegment", e);
        return false;
    }

    public void handleUnrecoverableError(CommitLogReadException e) throws IOException
    {
        logger.error("CommitLog unrecoverable error", e);
        statusTracker.requestTermination();
        stats.corruptCommitLog(log);
        throw e;
    }

    public void handleMutation(Mutation mutation, int size, int mutationPosition, @Nullable CommitLogDescriptor desc)
    {
        if (!mutation.trackedByCDC())
        {
            logger.debug("Ignore mutation not tracked by CDC");
            return;
        }

        mutation.getPartitionUpdates()
                .stream()
                .filter(update -> this.filter(mutationPosition, update))
                .map(update -> Pair.of(update, maxTimestamp(update)))
                .filter(this::withinTimeWindow)
                .peek(pair -> pair.getLeft().validate())
                .map(this::toCdcUpdate)
                .forEach(updates::add);
    }

    /**
     * @param update PartitionUpdate
     * @return max timestamp for a given PartitionUpdate
     */
    private long maxTimestamp(PartitionUpdate update)
    {
        // row deletion
        if (update.rowCount() == 1 && !update.lastRow().deletion().isLive())
        {
            return update.lastRow().deletion().time().markedForDeleteAt();
        }
        else
        {
            return update.maxTimestamp();
        }
    }

    private PartitionUpdateWrapper toCdcUpdate(Pair<PartitionUpdate, Long> update)
    {
        return toCdcUpdate(update.getLeft(), update.getRight());
    }

    private PartitionUpdateWrapper toCdcUpdate(PartitionUpdate update,
                                               long maxTimestampMicros)
    {
        return new FourZeroPartitionUpdateWrapper(update,
                                                  maxTimestampMicros,
                                                  executor);
    }

    /**
     * @param position current position in CommitLog.
     * @param update   the partition update
     * @return true if this is a mutation we are looking for.
     */
    private boolean filter(int position, PartitionUpdate update)
    {
        return isCdcEnabled(update) && withinRange(position, update);
    }

    private boolean isCdcEnabled(PartitionUpdate update)
    {
        if (update.metadata().params.cdc)
        {
            return true;
        }

        String keyspace = getKeyspace(update);
        String table = getTable(update);
        logger.debug("Ignore partition update from table not tracked by CDC",
                     "keyspace", keyspace, "table", table);
        stats.untrackedChangesIgnored(keyspace, table, 1);
        return false;
    }

    private boolean withinTimeWindow(Pair<PartitionUpdate, Long> update)
    {
        PartitionUpdate pu = update.getLeft();
        long mutationTimestamp = update.getRight();
        boolean shouldInclude = withinTimeWindow(mutationTimestamp);
        if (!shouldInclude)
        {
            String keyspace = getKeyspace(pu);
            String table = getTable(pu);
            if (logger.isTraceEnabled())
            {
                logger.trace("Exclude the update due to out of the allowed time window.",
                             "update", "'" + pu + '\'',
                             "keyspace", keyspace, "table", table,
                             "timestampMicros", mutationTimestamp,
                             "maxAgeMicros", startTimestampMicros == null ? "null" : startTimestampMicros);
            }
            else
            {
                logger.warn("Exclude the update due to out of the allowed time window.", null,
                            "keyspace", keyspace, "table", table,
                            "timestampMicros", mutationTimestamp,
                            "maxAgeMicros", startTimestampMicros == null ? "null" : startTimestampMicros);
            }
            stats.droppedOldMutation(getKeyspace(pu), getTable(pu), mutationTimestamp);
            return false;
        }
        return true;
    }

    private boolean withinTimeWindow(long mutationTimestamp)
    {
        if (startTimestampMicros == null)
        {
            return true;
        }
        return mutationTimestamp >= startTimestampMicros;
    }

    /**
     * @param position current position in CommitLog.
     * @param update   a CommitLog PartitionUpdate.
     * @return true if PartitionUpdate overlaps with the Spark worker token range.
     */
    private boolean withinRange(int position, PartitionUpdate update)
    {
        if (tokenRange == null)
        {
            return true;
        }

        BigInteger token = TokenUtils.tokenToBigInteger(update.partitionKey().getToken());

        if (tokenRange.contains(token))
        {
            Marker marker = log.markerAt(segmentId, position);
            if (markers.canIgnore(marker, token))
            {
                logger.debug("Ignoring mutation before start marker", "position", position, "marker", marker.position(), "markerSegmentId", marker.segmentId());
                return false;
            }
            return true;
        }

        String keyspace = getKeyspace(update);
        String table = getTable(update);
        logger.debug("Ignore out of range partition update.",
                     "keyspace", keyspace, "table", table);
        stats.outOfTokenRangeChangesIgnored(keyspace, table, 1);
        return false;
    }

    private static String getKeyspace(PartitionUpdate partitionUpdate)
    {
        return partitionUpdate.metadata().keyspace;
    }

    private static String getTable(PartitionUpdate partitionUpdate)
    {
        return partitionUpdate.metadata().name;
    }
}

