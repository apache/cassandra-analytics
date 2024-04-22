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

package org.apache.cassandra.spark.bulkwriter;

import java.io.Serializable;
import java.math.BigInteger;

import com.google.common.collect.Range;

public class StreamError implements Serializable
{
    private static final long serialVersionUID = 8897970306271012424L;
    public final RingInstance instance;
    public final String errMsg;
    public final Range<BigInteger> failedRange;

    public StreamError(Range<BigInteger> failedRange, RingInstance instance, String errMsg)
    {
        // todo: range should be all open-closed, but it is not consistent in the project yet. Enable the check later
//        Preconditions.checkArgument(RangeUtils.isOpenClosedRange(failedRange), "Token range is not open-closed");
        this.failedRange = failedRange;
        this.instance = instance;
        this.errMsg = errMsg;
    }

    @Override
    public String toString()
    {
        return "StreamError{instance:'" + instance
               + "',failedRange:'" + failedRange
               + "',errMsg:'" + errMsg + "'}";
    }
}
