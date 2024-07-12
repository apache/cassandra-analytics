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

package org.apache.cassandra.spark.reader.common;

/**
 * A GC friendly long[].
 * Allocating large arrays (that are not short-lived) generate fragmentation
 * in old-gen space. This breaks such large long array into fixed size pages
 * to avoid that problem.
 */
public class BigLongArray
{
    static final int DEFAULT_PAGE_SIZE = 4096;

    private final long[][] pages;
    public final int size;
    private final int pageSize;

    public BigLongArray(int size)
    {
        if (size < 0)
        {
            throw new IndexOutOfBoundsException(String.format("BigLongArray size cannot be less than 0: %d)", size));
        }
        this.size = size;
        this.pageSize = DEFAULT_PAGE_SIZE;

        int lastPageSize = size % pageSize;
        int fullPageCount = size / pageSize;
        int pageCount = fullPageCount + (lastPageSize == 0 ? 0 : 1);
        pages = new long[pageCount][];

        for (int page = 0; page < fullPageCount; ++page)
        {
            pages[page] = new long[pageSize];
        }

        if (lastPageSize != 0)
        {
            pages[pages.length - 1] = new long[lastPageSize];
        }
    }

    public void set(int index, long value)
    {
        checkIdx(index);
        int page = index / pageSize;
        int pageIdx = index % pageSize;
        pages[page][pageIdx] = value;
    }

    public long get(int index)
    {
        checkIdx(index);
        int page = index / pageSize;
        int pageIdx = index % pageSize;
        return pages[page][pageIdx];
    }

    private void checkIdx(int index)
    {
        if (index < 0 || size < index)
        {
            throw new IndexOutOfBoundsException(String.format("%d is not within [0, %d)", index, size));
        }
    }
}
