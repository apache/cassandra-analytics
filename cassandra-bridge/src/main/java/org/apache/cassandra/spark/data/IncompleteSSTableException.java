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

package org.apache.cassandra.spark.data;

import java.util.Arrays;

import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.jetbrains.annotations.Nullable;

/**
 * Thrown when a snapshot of SSTables is incomplete so cannot be used
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class IncompleteSSTableException extends SSTableStreamException
{
    public IncompleteSSTableException(FileType... fileTypes)
    {
        super(String.format("SSTable file component '%s' is required but could not be found",
                            Arrays.toString(fileTypes)));
    }

    public static boolean isIncompleteException(@Nullable Throwable throwable)
    {
        return throwable != null && (throwable instanceof IncompleteSSTableException || isIncompleteException(throwable.getCause()));
    }
}
