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
import org.apache.cassandra.utils.FBUtilities;
import org.jetbrains.annotations.NotNull;

/**
 * Copied and refactored from org.apache.cassandra.db.commitlog.CommitLogSegmentReader to deserialize CommitLog segments.
 * 'seek' method added so we can efficiently seek to previous location without reading and deserializing previous segments.
 */
public class SeekableCommitLogSegmentReader implements Iterable<CommitLogSegmentReader.SyncSegment>
{
    private final CommitLogReadHandler handler;
    private final CommitLogDescriptor descriptor;
    private final RandomAccessReader reader;
    private final CommitLogSegmentReader.Segmenter segmenter;
    private final LoggerHelper logger;
    private final boolean tolerateTruncation;

    /**
     * Ending position of the current sync section
     */
    protected int end;

    protected SeekableCommitLogSegmentReader(CommitLogReadHandler handler,
                                             CommitLogDescriptor descriptor,
                                             RandomAccessReader reader,
                                             LoggerHelper logger,
                                             boolean tolerateTruncation)
    {
        this.handler = handler;
        this.descriptor = descriptor;
        this.reader = reader;
        this.logger = logger;
        this.tolerateTruncation = tolerateTruncation;

        end = (int) reader.getFilePointer();
        if (descriptor.getEncryptionContext().isEnabled())
        {
            throw new UnsupportedOperationException("Encrypted CommitLogs currently not supported");
        }
        else if (descriptor.compression != null)
        {
            segmenter = new CommitLogSegmentReader.CompressedSegmenter(descriptor, reader);
        }
        else
        {
            segmenter = new CommitLogSegmentReader.NoOpSegmenter(reader);
        }
    }

    public void seek(int newPosition)
    {
        // The SegmentIterator will seek to the new position in the reader when readSyncMarker is next called
        end = newPosition;
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
                    end = readSyncMarker(descriptor, currentStart, reader);
                    if (end < 0)
                    {
                        return endOfData();
                    }
                    if (end > reader.length())
                    {
                        // The CRC was good (meaning it was good when it was written and still looks legit),
                        // but the file is truncated now. Try to grab and use as much of the file as possible,
                        // which might be nothing if the end of the file truly is corrupt.
                        end = (int) reader.length();
                    }
                    return segmenter.nextSegment(currentStart + CommitLogSegment.SYNC_MARKER_SIZE, end);
                }
                catch (CommitLogSegmentReader.SegmentReadException exception)
                {
                    try
                    {
                        handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(
                                exception.getMessage(),
                                CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                !exception.invalidCrc && tolerateTruncation));
                    }
                    catch (IOException ioException)
                    {
                        throw new RuntimeException(ioException);
                    }
                }
                catch (IOException exception)
                {
                    try
                    {
                        boolean tolerateErrorsInSection = tolerateTruncation & segmenter.tolerateSegmentErrors(end, reader.length());
                        // If no exception is thrown, the while loop will continue
                        handler.handleUnrecoverableError(new CommitLogReadHandler.CommitLogReadException(
                                exception.getMessage(),
                                CommitLogReadHandler.CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                tolerateErrorsInSection));
                    }
                    catch (IOException ioException)
                    {
                        throw new RuntimeException(ioException);
                    }
                }
            }
        }
    }

    private int readSyncMarker(CommitLogDescriptor descriptor, int offset, RandomAccessReader reader) throws IOException
    {
        if (offset > reader.length() - CommitLogSegment.SYNC_MARKER_SIZE)
        {
            // There was no room in the segment to write a final header. No data could be present here.
            return -1;
        }
        long current = reader.getFilePointer();
        if (offset != current)
        {
            long timeNanos = System.nanoTime();
            reader.seek(offset);
            logger.info("Seek to position", "from", current, "to", offset, "timeNanos", System.nanoTime() - timeNanos);
        }
        CRC32 crc = new CRC32();
        FBUtilities.updateChecksumInt(crc, (int) (descriptor.id & 0xFFFFFFFFL));
        FBUtilities.updateChecksumInt(crc, (int) (descriptor.id >>> 32));
        FBUtilities.updateChecksumInt(crc, (int) reader.getPosition());
        int end = reader.readInt();
        long fileCRC = reader.readInt() & 0xFFFFFFFFL;
        if (crc.getValue() != fileCRC)
        {
            if (end != 0 || fileCRC != 0)
            {
                String message = String.format("Encountered bad header at position %d of commit log %s, with invalid CRC. "
                                             + "The end of segment marker should be zero.", offset, reader.getPath());
                throw new CommitLogSegmentReader.SegmentReadException(message, true);
            }
            return -1;
        }
        else if (end < offset || reader.length() < end)
        {
            String message = String.format("Encountered bad header at position %d of commit log %s, with bad position but valid CRC",
                                           offset, reader.getPath());
            throw new CommitLogSegmentReader.SegmentReadException(message, false);
        }
        return end;
    }

    @NotNull
    public Iterator<CommitLogSegmentReader.SyncSegment> iterator()
    {
        return new SegmentIterator();
    }
}
