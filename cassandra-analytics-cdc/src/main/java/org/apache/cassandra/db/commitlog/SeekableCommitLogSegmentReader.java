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

import java.io.IOException;
import java.util.Iterator;
import java.util.zip.CRC32;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.spark.utils.LoggerHelper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.SYNC_MARKER_SIZE;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Copied and refactored from org.apache.cassandra.db.commitlog.CommitLogSegmentReader to deserialize CommitLog segments.
 * 'seek' method added, so we can efficiently seek to previous location without reading and deserializing previous segments.
 */
public class SeekableCommitLogSegmentReader implements Iterable<CommitLogSegmentReader.SyncSegment>
{
    private final CommitLogReadHandler handler;
    private final RandomAccessReader reader;
    private final CommitLogSegmentReader.Segmenter segmenter;
    private final LoggerHelper logger;
    private final boolean tolerateTruncation;
    private final long segmentId;

    /**
     * ending position of the current sync section.
     */
    protected int end;

    protected SeekableCommitLogSegmentReader(long segmentId,
                                             CommitLogReadHandler handler,
                                             @Nullable CommitLogDescriptor desc,
                                             RandomAccessReader reader,
                                             LoggerHelper logger,
                                             boolean tolerateTruncation)
    {
        this.segmentId = segmentId;
        this.handler = handler;
        this.reader = reader;
        this.logger = logger;
        this.tolerateTruncation = tolerateTruncation;

        end = (int) reader.getFilePointer();
        if (desc != null && desc.getEncryptionContext().isEnabled())
        {
            throw new UnsupportedOperationException("Encrypted CommitLogs currently not supported");
        }
        else if (desc != null && desc.compression != null)
        {
            logger.trace("Opening CompressedSegmenter reader");
            segmenter = new CommitLogSegmentReader.CompressedSegmenter(desc, reader);
        }
        else
        {
            logger.trace("Opening NoOpSegmenter reader");
            segmenter = new CommitLogSegmentReader.NoOpSegmenter(reader);
        }
    }

    public void seek(int newPosition)
    {
        // the SegmentIterator will seek to the new position in the reader when readSyncMarker is next called.
        logger.trace("Seeking to position", "position", newPosition);
        this.end = newPosition;
    }

    protected class SegmentIterator extends AbstractIterator<CommitLogSegmentReader.SyncSegment>
    {
        protected CommitLogSegmentReader.SyncSegment computeNext()
        {
            while (true)
            {
                try
                {
                    int currentStart = end;
                    end = readSyncMarker(currentStart, reader);
                    if (end == -1)
                    {
                        return endOfData();
                    }
                    if (end > reader.length())
                    {
                        // the CRC was good (meaning it was good when it was written and still looks legit), but the file is truncated now.
                        // try to grab and use as much of the file as possible, which might be nothing if the end of the file truly is corrupt
                        end = (int) reader.length();
                    }
                    return segmenter.nextSegment(currentStart + SYNC_MARKER_SIZE, end);
                }
                catch (CommitLogSegmentReader.SegmentReadException e)
                {
                    try
                    {
                        handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(
                        e.getMessage(),
                        CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                        !e.invalidCrc && tolerateTruncation));
                    }
                    catch (IOException ioe)
                    {
                        throw new RuntimeException(ioe);
                    }
                }
                catch (IOException e)
                {
                    try
                    {
                        boolean tolerateErrorsInSection = tolerateTruncation & segmenter.tolerateSegmentErrors(end, reader.length());
                        // if no exception is thrown, the while loop will continue
                        handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(
                        e.getMessage(),
                        CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                        tolerateErrorsInSection));
                    }
                    catch (IOException ioe)
                    {
                        throw new RuntimeException(ioe);
                    }
                }
            }
        }
    }

    private int readSyncMarker(int offset, RandomAccessReader reader) throws IOException
    {
        if (offset > reader.length() - SYNC_MARKER_SIZE)
        {
            // There was no room in the segment to write a final header. No data could be present here.
            return -1;
        }
        long current = reader.getFilePointer();
        if (offset != current)
        {
            long timeNanos = System.nanoTime();
            reader.seek(offset);
            logger.debug("Seek to position", "from", current, "to", offset, "timeNanos", System.nanoTime() - timeNanos);
        }
        CRC32 crc = new CRC32();
        updateChecksumInt(crc, (int) (segmentId & 0xFFFFFFFFL));
        updateChecksumInt(crc, (int) (segmentId >>> 32));
        updateChecksumInt(crc, (int) reader.getPosition());
        int end = reader.readInt();
        long filecrc = reader.readInt() & 0xffffffffL;
        if (crc.getValue() != filecrc)
        {
            if (end != 0 || filecrc != 0)
            {
                String msg = String.format("Encountered bad header at position %d of commit log %s, with invalid CRC. " +
                                           "The end of segment marker should be zero.", offset, reader.getPath());
                throw new CommitLogSegmentReader.SegmentReadException(msg, true);
            }
            return -1;
        }
        else if (end < offset || end > reader.length())
        {
            String msg = String.format("Encountered bad header at position %d of commit log %s, with bad position but valid CRC", offset, reader.getPath());
            throw new CommitLogSegmentReader.SegmentReadException(msg, false);
        }
        return end;
    }

    @NotNull
    public Iterator<CommitLogSegmentReader.SyncSegment> iterator()
    {
        return new SegmentIterator();
    }
}
