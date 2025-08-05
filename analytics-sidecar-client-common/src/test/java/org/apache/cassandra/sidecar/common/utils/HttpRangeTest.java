/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.utils;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.exceptions.RangeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RangeTest
 */
public class HttpRangeTest
{
    @Test
    public void testValidPartialRange()
    {
        String rangeHeaderVal = "bytes=2-";
        HttpRange range = HttpRange.parseHeader(rangeHeaderVal, 5);
        assertThat(range.length()).isEqualTo(3);
        assertThat(range.start()).isEqualTo(2);
        assertThat(range.end()).isEqualTo(4);

        rangeHeaderVal = "bytes=-100";
        range = HttpRange.parseHeader(rangeHeaderVal, 5);
        assertThat(range.length()).isEqualTo(5);
        assertThat(range.start()).isEqualTo(0);
        assertThat(range.end()).isEqualTo(4);

        rangeHeaderVal = "bytes=-100";
        range = HttpRange.parseHeader(rangeHeaderVal, 200);
        assertThat(range.length()).isEqualTo(100);
        assertThat(range.start()).isEqualTo(100);
        assertThat(range.end()).isEqualTo(199);

        range = HttpRange.parseHeader(null, 200);
        assertThat(range.length()).isEqualTo(200);
        assertThat(range.start()).isEqualTo(0);
        assertThat(range.end()).isEqualTo(199);
    }

    @Test
    public void testValidFullRange()
    {
        final String rangeHeaderVal = "bytes=0-100";
        final HttpRange range = HttpRange.parseHeader(rangeHeaderVal, 500);
        assertThat(range.length()).isEqualTo(101);
        assertThat(range.start()).isEqualTo(0);
        assertThat(range.end()).isEqualTo(100);
    }

    @Test
    public void testInvalidRangeFormat()
    {
        String rangeHeader = "bytes=2344--3432";
        String msg = "Invalid range header: bytes=2344--3432. Supported Range formats are bytes=<start>-<end>, " +
                     "bytes=<start>-, bytes=-<suffix-length>";
        assertThatThrownBy(() -> {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessage(msg);

        msg = "Invalid range header: bytes=-. Supported Range formats are bytes=<start>-<end>, " +
              "bytes=<start>-, bytes=-<suffix-length>";
        assertThatThrownBy(() -> {
            HttpRange.parseHeader("bytes=-", Long.MAX_VALUE);
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessage(msg);
    }

    @Test
    public void testInvalidSuffixLength()
    {
        final String rangeHeader = "bytes=-0";
        String msg = "Range does not satisfy boundary requirements. range=[9223372036854775807, 9223372036854775806]";
        assertThatThrownBy(() -> {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessage(msg);
    }

    @Test
    public void testInvalidRangeBoundary()
    {
        final String rangeHeader = "bytes=9-2";
        String msg = "Range does not satisfy boundary requirements. range=[9, 2]";
        assertThatThrownBy(() -> {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessage(msg);
    }

    @Test
    public void testWrongRangeUnitUsed()
    {
        final String rangeVal = "bits=0-";
        String msg = "Invalid range header: bits=0-. Supported Range formats are bytes=<start>-<end>, " +
                     "bytes=<start>-, bytes=-<suffix-length>";
        assertThatThrownBy(() -> {
            HttpRange.parseHeader(rangeVal, 5);
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessage(msg);
    }

    @Test
    public void testToString()
    {
        final String rangeHeaderVal = "bytes=0-100";
        final HttpRange range = HttpRange.parseHeader(rangeHeaderVal, Long.MAX_VALUE);
        assertThat(range.toString()).isEqualTo("bytes=0-100");
    }

    @Test
    public void testInvalidRangeBoundValueInHeader()
    {
        // the right end of range is larger than long
        final String rangeHeader = "bytes=0-1" + Long.MAX_VALUE;
        String msg = "Invalid range header: bytes=0-19223372036854775807. Supported Range formats are " +
                     "bytes=<start>-<end>, bytes=<start>-, bytes=-<suffix-length>";
        assertThatThrownBy(() -> {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        }).isInstanceOf(IllegalArgumentException.class)
          .hasMessage(msg);
    }

    @Test
    public void testIntersect()
    {
        HttpRange range1;
        HttpRange range2;
        HttpRange expected;
        range1 = HttpRange.of(5, 10);
        range2 = HttpRange.of(9, 15);
        expected = HttpRange.of(9, 10);
        assertThat(range1.intersect(range2)).isEqualTo(expected);
        assertThat(range2.intersect(range1)).isEqualTo(expected);

        range1 = HttpRange.of(1, 5);
        range2 = HttpRange.of(5, 15);
        expected = HttpRange.of(5, 5);
        assertThat(range1.intersect(range2)).isEqualTo(expected);
        assertThat(range2.intersect(range1)).isEqualTo(expected);

        range1 = HttpRange.of(1, 15);
        range2 = HttpRange.of(5, 10);
        expected = HttpRange.of(5, 10);
        assertThat(range1.intersect(range2)).isEqualTo(expected);
        assertThat(range2.intersect(range1)).isEqualTo(expected);
    }

    @Test
    public void testRangesDoNotIntersect()
    {
        HttpRange range1 = HttpRange.of(1, 5);
        HttpRange range2 = HttpRange.of(9, 15);

        assertThatThrownBy(() -> range1.intersect(range2)).isInstanceOf(RangeException.class);
    }
}
