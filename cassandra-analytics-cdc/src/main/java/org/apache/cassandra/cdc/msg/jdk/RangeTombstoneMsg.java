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

package org.apache.cassandra.cdc.msg.jdk;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.cdc.msg.RangeTombstone;

public class RangeTombstoneMsg
{
    private final List<Column> startBound;
    private final List<Column> endBound;
    public final boolean startInclusive;
    public final boolean endInclusive;

    public RangeTombstoneMsg(JdkMessageConverter valueConverter,
                             RangeTombstone tombstone)
    {
        this.startBound = tombstone.getStartBound().stream()
                                   .map(valueConverter::toCdcMessage)
                                   .collect(Collectors.toList());
        this.endBound = tombstone.getEndBound().stream()
                                 .map(valueConverter::toCdcMessage)
                                 .collect(Collectors.toList());
        this.startInclusive = tombstone.startInclusive;
        this.endInclusive = tombstone.endInclusive;
    }

    public List<Column> startBound()
    {
        return startBound;
    }

    public List<Column> endBound()
    {
        return endBound;
    }

    public boolean isStartInclusive()
    {
        return startInclusive;
    }

    public boolean isEndInclusive()
    {
        return endInclusive;
    }
}
